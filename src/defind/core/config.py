from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class OrchestratorConfig:
    """Configuration for the streaming orchestrator."""

    rpc_url: str
    address: str
    topic0s: list[str]
    start_block: int | str
    end_block: int | str
    step: int = 5_000
    concurrency: int = 16
    out_root: Path = Path("./data")
    rows_per_shard: int = 250_000
    batch_decode_rows: int = 10_000
    timeout_s: int = 20
    min_split_span: int = 2_000
    write_final_partial: bool = True


@dataclass(frozen=True)
class RawFetchConfig:
    """Configuration for the raw log fetcher (CLI)."""

    rpc_url: str
    contract: str
    events: list[str]
    from_block: int
    to_block: int
    step: int = 1_000
    concurrency: int = 16
    jsonl_out: str = ""
    manifest_path: str = ""
    rerun_failed: bool = False
