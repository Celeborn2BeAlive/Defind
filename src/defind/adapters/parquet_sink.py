from __future__ import annotations
import os, pyarrow as pa, pyarrow.parquet as pq
from typing import Iterable

from defind.domain.decoding import DECODED_SCHEMA
from ..ports.storage import EventSink
from ..domain.models import EventLog

_SCHEMA = pa.schema([
    ("address", pa.string()),
    ("topic0", pa.string()),
    ("data_hex", pa.string()),
    ("block_number", pa.int64()),
    ("tx_hash", pa.string()),
    ("log_index", pa.int64()),
])

def _events_to_table(events: Iterable[EventLog]) -> pa.Table:
    evs = list(events)
    if not evs:
        return pa.Table.from_arrays([pa.array([], type=f.type) for f in _SCHEMA], names=[f.name for f in _SCHEMA])
    return pa.Table.from_arrays(
        arrays=[
            pa.array([e.address for e in evs], pa.string()),
            pa.array([e.topic0 for e in evs], pa.string()),
            pa.array([e.data_hex for e in evs], pa.string()),
            pa.array([e.block_number for e in evs], pa.int64()),
            pa.array([e.tx_hash for e in evs], pa.string()),
            pa.array([e.log_index for e in evs], pa.int64()),
        ],
        names=[f.name for f in _SCHEMA],
    )

class ParquetEventSink(EventSink):
    def __init__(self, root_dir: str, addr_slug: str, topics_fp: str) -> None:
        self.root = root_dir
        self.addr_slug = addr_slug
        self.topics_fp = topics_fp
        os.makedirs(self.root, exist_ok=True)

    def _path(self, fb: int, tb: int) -> str:
        fname = f"{self.addr_slug}__topics-{self.topics_fp}__chunk_{fb}_{tb}.parquet"
        return os.path.join(self.root, fname)

    async def write_chunk(self, from_block: int, to_block: int, events: Iterable[EventLog]) -> None:
        path = self._path(from_block, to_block)
        tmp  = path + ".tmp"
        table = _events_to_table(events)
        pq.write_table(table, tmp, compression="snappy", use_dictionary=True)
        os.replace(tmp, path)


class DecodedShardWriter:
    """
    Writes decoded rows to shards with deterministic sort.
    """
    def __init__(self, out_dir: str, codec: str = "zstd") -> None:
        self.out_dir = out_dir
        self.shards_dir = os.path.join(out_dir, "shards")
        os.makedirs(self.shards_dir, exist_ok=True)
        self.codec = codec

    def next_shard_index(self) -> int:
        import glob, os
        existing = sorted(glob.glob(os.path.join(self.shards_dir, "shard_*.parquet")))
        if not existing:
            return 1
        last = os.path.basename(existing[-1]).split("_")[1].split(".")[0]
        return int(last) + 1

    def write_shard(self, arrays_by_name: dict[str, pa.Array], shard_idx: int) -> str:
        table = pa.Table.from_pydict(arrays_by_name, schema=DECODED_SCHEMA)
        table = table.sort_by([("block_number", "ascending"),
                               ("tx_hash", "ascending"),
                               ("log_index", "ascending")])
        out_path = os.path.join(self.shards_dir, f"shard_{shard_idx:05d}.parquet")
        pq.write_table(table, out_path, compression=self.codec)
        return out_path
