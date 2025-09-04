from __future__ import annotations
import os
import asyncio, time
from typing import Sequence
import pyarrow as pa
import pyarrow.parquet as pq

from defind.adapters.parquet_sink import DecodedShardWriter
from defind.domain.decoding import COLS, DECODED_SCHEMA, decode_events_table
from ..domain.models import ChunkRec
from ..domain.value_types import Address, Topic0, Status
from ..ports.rpc import RPCClient
from ..ports.storage import EventSink, ManifestSink
from .planning import plan_chunks, subtract_interval

async def fetch_decode_persist(
    *,
    rpc: RPCClient,
    sink: EventSink,
    manifest: ManifestSink,
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

    sem = asyncio.Semaphore(concurrency)

    async def run_leaf(fb: int, tb: int) -> None:
        nonlocal processed_ok, processed_failed, total_logs
        async with sem:
            try:
                logs = await rpc.get_logs(address, list(topic0s), fb, tb)
            except Exception as e:
                await manifest.append(ChunkRec(fb, tb, "failed", 1, str(e), 0, time.time()))
                processed_failed += 1
                return
        total_logs += len(logs)
        await sink.write_chunk(fb, tb, logs)
        await manifest.append(ChunkRec(fb, tb, "done", 1, None, len(logs), time.time()))
        processed_ok += 1

    async def run_chunk(fb: int, tb: int) -> None:
        stack: list[tuple[int,int]] = [(fb, tb)]
        while stack:
            a, b = stack.pop()
            try:
                await run_leaf(a, b)
            except Exception as e:
                span = b - a + 1
                if auto_split and span > min_split_span and (split_on_any_error or True):
                    mid = (a + b)//2
                    left, right = (a, mid), (mid+1, b)
                    if left[0] <= left[1]: stack.append(left)
                    if right[0] <= right[1]: stack.append(right)
                    nonlocal partially_covered_split
                    partially_covered_split += 1
                else:
                    await manifest.append(ChunkRec(a, b, "failed", 1, str(e), 0, time.time()))
                    nonlocal processed_failed
                    processed_failed += 1

    tasks = []
    for rec in plan_chunks(start_block, end_block, step):
        tasks.append(asyncio.create_task(run_chunk(rec.from_block, rec.to_block)))
    await asyncio.gather(*tasks)

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
