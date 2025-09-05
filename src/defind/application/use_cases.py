from __future__ import annotations
import os
import asyncio, time
from typing import Dict, Sequence
import pyarrow as pa
import pyarrow.parquet as pq

from defind.adapters.coverage_local import LocalManifestCoverage
from defind.adapters.manifest_jsonl import JSONLManifest
from defind.adapters.parquet_sink import DecodedShardWriter, ParquetEventSink
from defind.adapters.rpc_httpx import HttpxRPC
from defind.application.utils import _fp, _now_ts_str
from defind.domain.decoding import COLS, DECODED_SCHEMA, decode_events_table
from defind.ports.coverage import Coverage
from ..domain.models import ChunkRec
from ..domain.value_types import Address, Topic0, Status
from ..ports.rpc import RPCClient
from ..ports.storage import EventSink, ManifestSink
from .planning import plan_chunks, subtract_interval



def _subtract(iv: tuple[int, int], covered_merged: list[tuple[int, int]]) -> list[tuple[int, int]]:
    s, e = iv
    if s > e: return []
    if not covered_merged: return [iv]
    res: list[tuple[int, int]] = []
    cur = s
    for cs, ce in covered_merged:
        if ce < cur: continue
        if cs > e: break
        if cs > cur: res.append((cur, min(e, cs - 1)))
        cur = max(cur, ce + 1)
        if cur > e: break
    if cur <= e: res.append((cur, e))
    return res

async def fetch_decode_persist(
    *,
    rpc: RPCClient,
    sink: EventSink,
    manifest: ManifestSink,
    coverage: Coverage | None,  # ← NEW
    address: Address,
    topic0s: Sequence[Topic0],
    start_block: int, end_block: int,
    step: int,
    concurrency: int,
    auto_split: bool = True,
    min_split_span: int = 2_000,
    split_on_any_error: bool = True,
) -> dict[str, int]:
    processed_ok = processed_failed = total_logs = 0
    skipped_full_chunks = partially_covered_split = 0

    # 1) query coverage once at start
    covered_merged: list[tuple[int, int]] = []
    if coverage is not None:
        covered_merged = await coverage.covered_ranges()

    # 2) compute uncovered seeds
    uncovered_total: list[tuple[int, int]] = _subtract((start_block, end_block), covered_merged)
    if not uncovered_total:
        return {
            "processed_ok": 0,
            "processed_failed": 0,
            "executed_subranges": 0,
            "total_logs": 0,
            "skipped_full_chunks": 0,
            "partially_covered_split": 0,
        }

    seeds: list[tuple[int, int]] = []
    for us, ue in uncovered_total:
        b = us
        while b <= ue:
            fb, tb = b, min(ue, b + step - 1)
            seeds.append((fb, tb))
            b = tb + 1

    sem = asyncio.Semaphore(concurrency)

    async def append_manifest(fb: int, tb: int, status: Status, attempts: int, err: str | None, logs_cnt: int) -> None:
        await manifest.append(ChunkRec(
            from_block=fb, to_block=tb, status=status,
            attempts=attempts, error=err, logs=logs_cnt, updated_at=time.time()
        ))

    async def run_leaf(fb: int, tb: int) -> None:
        nonlocal processed_ok, processed_failed, total_logs, skipped_full_chunks
        # quick existence guard (handles races and mid-run restarts)
        if coverage is not None and await coverage.chunk_exists(fb, tb):
            skipped_full_chunks += 1
            return
        async with sem:
            try:
                logs = await rpc.get_logs(address, list(topic0s), fb, tb)
            except Exception as e:
                await append_manifest(fb, tb, "failed", 1, str(e), 0)
                processed_failed += 1
                return
        total_logs += len(logs)
        await sink.write_chunk(fb, tb, logs)
        await append_manifest(fb, tb, "done", 1, None, len(logs))
        processed_ok += 1

    async def run_chunk(fb: int, tb: int) -> None:
        nonlocal partially_covered_split, processed_failed
        stack: list[tuple[int, int]] = [(fb, tb)]
        while stack:
            a, b = stack.pop()
            try:
                await run_leaf(a, b)
            except Exception as e:
                span = b - a + 1
                if auto_split and span > min_split_span and (split_on_any_error or True):
                    mid = (a + b) // 2
                    left, right = (a, mid), (mid+1, b)
                    if left[0] <= left[1]: stack.append(left)
                    if right[0] <= right[1]: stack.append(right)
                    partially_covered_split += 1
                else:
                    await append_manifest(a, b, "failed", 1, str(e), 0)
                    processed_failed += 1

    await asyncio.gather(*(asyncio.create_task(run_chunk(fb, tb)) for (fb, tb) in seeds))

    return {
        "processed_ok": processed_ok,
        "processed_failed": processed_failed,
        "executed_subranges": processed_ok + processed_failed,
        "total_logs": total_logs,
        "skipped_full_chunks": skipped_full_chunks,
        "partially_covered_split": partially_covered_split,
    }




def decode_parquet_dir_to_shards(
    in_dir: str,
    out_dir: str,
    *,
    FILES_PER_BATCH: int = 64,
    ROWS_PER_SHARD: int = 250_000,
) -> list[str]:
    """
    Mirrors your notebook's write_shards_arrow() with clean types.
    Returns list of written shard paths.
    """
    os.makedirs(out_dir, exist_ok=True)
    writer = DecodedShardWriter(out_dir)
    shard_idx = writer.next_shard_index()

    buf: dict[str, list] = {name: [] for name in COLS}
    buffered = 0
    written: list[str] = []

    def _flush() -> None:
        nonlocal shard_idx, buffered, buf, written
        if buffered == 0:
            return
        arrays = {k: pa.array(v, type=DECODED_SCHEMA.field(k).type) for k, v in buf.items()}
        path = writer.write_shard(arrays, shard_idx)
        written.append(path)
        # reset
        buf = {name: [] for name in COLS}
        buffered = 0
        shard_idx += 1

    files = sorted([os.path.join(in_dir, f) for f in os.listdir(in_dir) if f.endswith(".parquet")])
    for i in range(0, len(files), FILES_PER_BATCH):
        batch = files[i:i+FILES_PER_BATCH]
        print(f"Decoding batch {i//FILES_PER_BATCH+1} [{i}..{i+len(batch)-1}] ({len(batch)} files)")
        for fpath in batch:
            table = pq.read_table(fpath).combine_chunks()
            cols = decode_events_table(table)
            if not cols:
                continue
            n = len(cols["tx_hash"])
            for k in COLS:
                buf[k].extend(cols[k])
            buffered += n
            if buffered >= ROWS_PER_SHARD:
                _flush()
    _flush()
    return written


async def fetch_logs_range_parquet(
    *,
    rpc_url: str,
    address: str,
    topic0s: list[str],
    start_block: int | str,      # supports 'earliest'/'genesis'
    end_block: int | str,        # supports 'latest'
    step: int,
    concurrency: int,
    out_dir: str,
) -> Dict[str, int]:
    """
    Writes Parquet chunks and a NEW per-run JSONL manifest.
    Coverage/skip is computed from ALL prior manifests in the same key_dir.
    """
    addr_slug = address.lower()
    topics_fp = _fp(topic0s)

    key_dir = os.path.join(out_dir, f"{addr_slug}__topics-{topics_fp}")
    man_dir = os.path.join(key_dir, "manifests")
    os.makedirs(man_dir, exist_ok=True)

    rpc = HttpxRPC(rpc_url)
    s_block = 0 if (isinstance(start_block, str) and start_block.lower() in ("earliest", "genesis")) else int(start_block)
    e_block = await rpc.latest_block() if (isinstance(end_block, str) and end_block.lower() == "latest") else int(end_block)
    if s_block > e_block:
        raise ValueError(f"start_block ({s_block}) must be <= end_block ({e_block})")

    # NEW: per-run manifest filename (timestamped; includes parameters)
    run_basename = f"run_{_now_ts_str()}_{addr_slug}_{topics_fp}_{s_block}_{e_block}.jsonl"
    manifest_path = os.path.join(man_dir, run_basename)

    sink = ParquetEventSink(key_dir, addr_slug, topics_fp)
    manifest = JSONLManifest(manifest_path)

    # Coverage from manifests (exclude current run's manifest)
    coverage = LocalManifestCoverage(man_dir, exclude_run_basename=run_basename)

    return await fetch_decode_persist(
        rpc=rpc,
        sink=sink,
        manifest=manifest,
        coverage=coverage,
        address=Address(addr_slug),
        topic0s=[Topic0(t) for t in topic0s],
        start_block=s_block,
        end_block=e_block,
        step=step,
        concurrency=concurrency,
    )

