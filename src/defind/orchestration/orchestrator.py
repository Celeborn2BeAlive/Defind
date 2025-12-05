"""Streaming orchestrator: fetch → decode → write universal shards.

Features
--------
- Resumable via *live manifest* + historical coverage merging.
- Dynamic projections: any registry projection key becomes a column.
- Bounded memory via `rows_per_shard` and `batch_decode_rows`.
- Automatic interval splitting on failures (`min_split_span`).

Public API
----------
`fetch_decode(...)` → dict[str, int] summary.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path

from defind.clients.rpc import RPC
from defind.core.config import OrchestratorConfig
from defind.core.models import ChunkRecord, Column, EventLog, Meta
from defind.decoding.decoder import decode_event
from defind.decoding.specs import EventRegistry
from defind.orchestration.utils import (
    iter_chunks,
    load_done_coverage,
    subtract_iv,
    topics_fingerprint,
)
from defind.storage.manifest import LiveManifest
from defind.storage.shards import ShardsDir, ShardWriter

# -------------------------
# Constants
# -------------------------

# RPC connection pool sizing: ensure enough connections for concurrent workers
# Formula: max(minimum_safe_value, 2 * concurrency) to handle request bursts
MIN_RPC_CONNECTIONS = 32

# -------------------------
# Context & worker
# -------------------------


@dataclass(kw_only=True)
class ProcessStats:
    processed_ok = 0
    processed_failed = 0
    executed_subranges = 0
    total_logs = 0
    partially_covered_split = 0
    shards_written = 0


@dataclass(slots=True)
class ProcessContext:
    """Shared state for interval processing (keeps worker signature small)."""

    rpc: RPC
    address: str
    topic0s: list[str]
    registry: EventRegistry
    batch_decode_rows: int
    min_split_span: int
    sem: asyncio.Semaphore
    agg_lock: asyncio.Lock
    manifest: LiveManifest
    aggregator: ShardWriter
    stats: ProcessStats


async def _process_logs(ctx: ProcessContext, logs: list[EventLog]) -> tuple[int, int, int]:
    """Decode logs and write to aggregator, returning (decoded, filtered, shards_written)."""
    buf = Column.empty()
    rows_in_batch = 0
    decoded_rows = 0
    filtered_rows = 0
    shards_written = 0

    for ev in logs:
        if not ev.topics:
            filtered_rows += 1
            continue

        # Decode raw hex data payload
        data_hex = ev.data_hex[2:] if ev.data_hex.lower().startswith("0x") else ev.data_hex
        data_bytes = bytes.fromhex(data_hex) if data_hex else b""

        meta = Meta(ev.block_number, ev.block_timestamp, ev.tx_hash, ev.log_index, ev.address)

        pe = decode_event(topics=ev.topics, data=data_bytes, meta=meta, registry=ctx.registry)
        if pe is None:
            filtered_rows += 1
            continue

        # Append all dynamic keys in one go
        buf.append_from_parsed(pe_name=pe.name, meta=meta, values=pe.values, contract_addr=pe.pool)
        rows_in_batch += 1
        decoded_rows += 1

        if rows_in_batch >= ctx.batch_decode_rows:
            async with ctx.agg_lock:
                written = ctx.aggregator.add(buf)
            shards_written += len(written)
            buf = Column.empty()
            rows_in_batch = 0

    # Flush last partial batch for this interval
    if rows_in_batch > 0:
        async with ctx.agg_lock:
            written = ctx.aggregator.add(buf)
        shards_written += len(written)

    return decoded_rows, filtered_rows, shards_written


async def process_interval(ctx: ProcessContext, start_block: int, end_block: int) -> None:
    """Process one inclusive [start_block, end_block] range with retry splitting."""
    stack: list[tuple[int, int]] = [(start_block, end_block)]
    while stack:
        a, b = stack.pop()
        await ctx.manifest.append(_create_started_record(a, b))
        try:
            async with ctx.sem:
                logs = await ctx.rpc.get_logs(address=ctx.address, topic0s=ctx.topic0s, from_block=a, to_block=b)

            ctx.stats.executed_subranges += 1
            ctx.stats.total_logs += len(logs)

            decoded_rows, filtered_rows, shards_here = await _process_logs(ctx, logs)

            ctx.stats.processed_ok += 1
            ctx.stats.shards_written += shards_here
            await ctx.manifest.append(_create_done_record(a, b, len(logs), decoded_rows, shards_here, filtered_rows))

        except Exception as e:
            span = b - a + 1
            if span > ctx.min_split_span:
                mid = (a + b) // 2
                stack.extend([(a, mid), (mid + 1, b)])
                ctx.stats.partially_covered_split += 1
                await ctx.manifest.append(_create_failed_record(a, b, str(e)))
            else:
                ctx.stats.processed_failed += 1
                await ctx.manifest.append(_create_failed_record(a, b, str(e)))


# -------------------------
# Helper factories
# -------------------------


def _create_started_record(a: int, b: int) -> ChunkRecord:
    """Create a 'started' chunk record."""
    return ChunkRecord(a, b, "started", 0, None, 0, 0, 0, time.time(), 0)


def _create_done_record(a: int, b: int, logs: int, decoded: int, shards: int, filtered: int) -> ChunkRecord:
    """Create a 'done' chunk record."""
    return ChunkRecord(a, b, "done", 1, None, logs, decoded, shards, time.time(), filtered)


def _create_failed_record(a: int, b: int, error: str) -> ChunkRecord:
    """Create a 'failed' chunk record."""
    return ChunkRecord(a, b, "failed", 1, error, 0, 0, 0, time.time(), 0)


# -------------------------
# Orchestrator
# -------------------------


@dataclass(kw_only=True)
class SetupDirectoriesResult:
    key_dir: Path
    manifests_dir: Path


def _setup_directories(out_root: Path, address: str, topic0s: list[str]) -> SetupDirectoriesResult:
    """Setup output directories for the fetch operation.

    Returns:
        Tuple of (key_dir, manifests_dir)
    """
    address_lc = address.lower()
    topics_fp = topics_fingerprint(topic0s)
    key_dir = out_root / f"{address_lc}__topics-{topics_fp}__universal"
    manifests_dir = key_dir / "manifests"
    manifests_dir.mkdir(exist_ok=True, parents=True)
    return SetupDirectoriesResult(
        key_dir=key_dir,
        manifests_dir=manifests_dir,
    )


async def _resolve_block_range(rpc: RPC, start_block: int | str, end_block: int | str) -> tuple[int, int]:
    """Resolve start and end blocks, handling special values like 'latest'.

    Returns:
        Tuple of (start, end) as integers
    """
    start = 0 if (isinstance(start_block, str) and start_block.lower() in ("earliest", "genesis")) else int(start_block)
    end = await rpc.latest_block() if (isinstance(end_block, str) and end_block.lower() == "latest") else int(end_block)

    if start > end:
        raise ValueError("start_block must be <= end_block")

    return start, end


def _build_work_seeds(start: int, end: int, step: int, covered: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Build list of uncovered block ranges to process.

    Returns:
        List of (start_block, end_block) tuples to process
    """
    seeds: list[tuple[int, int]] = []
    for uncovered_start, uncovered_end in subtract_iv((start, end), covered):
        for a, b in iter_chunks(uncovered_start, uncovered_end, step):
            seeds.append((a, b))
    return seeds


@dataclass(kw_only=True)
class FetchDecodeOutput:
    stats: ProcessStats
    key_dir: Path
    manifests_dir: Path
    shards_dir: ShardsDir


async def fetch_decode(
    *,
    config: OrchestratorConfig,
    registry: EventRegistry,
) -> FetchDecodeOutput:
    """Stream logs from RPC, decode with registry, and write Parquet shards.

    Returns
    -------
    FetchDecodeOutput
        Summary counters for the run and output files
    """
    # Setup directories and RPC client
    setup_directories_result = _setup_directories(config.out_root, config.address, config.topic0s)
    key_dir = setup_directories_result.key_dir
    manifests_dir = setup_directories_result.manifests_dir
    rpc = RPC(
        config.rpc_url, timeout_s=config.timeout_s, max_connections=max(MIN_RPC_CONNECTIONS, 2 * config.concurrency)
    )

    try:
        # Resolve block range
        start, end = await _resolve_block_range(rpc, config.start_block, config.end_block)

        # Setup manifest and coverage
        address_lc = config.address.lower()
        topics_fp = topics_fingerprint(config.topic0s)
        run_basename = (
            f"run_{time.strftime('%Y%m%d_%H%M%S', time.gmtime())}_{address_lc}_{topics_fp}_{start}_{end}.jsonl"
        )
        manifest = LiveManifest(manifests_dir / run_basename)
        covered = load_done_coverage(manifests_dir, exclude_basename=run_basename)

        # Build work list
        seeds = _build_work_seeds(start, end, config.step, covered)

        shards_dir = ShardsDir(
            out_root=config.out_root,
            addr_slug=address_lc,
            topics_fp=topics_fp,
        )
        output = FetchDecodeOutput(
            stats=ProcessStats(),
            key_dir=key_dir,
            manifests_dir=manifests_dir,
            shards_dir=shards_dir,
        )

        if not seeds:
            return output

        aggregator = ShardWriter(
            shards_dir,
            rows_per_shard=config.rows_per_shard,
            write_final_partial=config.write_final_partial,
        )
        agg_lock = asyncio.Lock()
        sem = asyncio.Semaphore(config.concurrency)

        ctx = ProcessContext(
            rpc=rpc,
            address=config.address,
            topic0s=config.topic0s,
            registry=registry,
            batch_decode_rows=config.batch_decode_rows,
            min_split_span=config.min_split_span,
            sem=sem,
            agg_lock=agg_lock,
            manifest=manifest,
            aggregator=aggregator,
            stats=output.stats,
        )

        # Dispatch workers
        tasks = [asyncio.create_task(process_interval(ctx, a, b)) for a, b in seeds]

        await asyncio.gather(*tasks)

        # Close aggregator and possibly write trailing partial shard
        async with agg_lock:
            last = aggregator.close()
        if last:
            output.stats.shards_written += 1

        return output
    finally:
        await rpc.aclose()
