"""Core data models and dynamic column buffer (universal layout).

This module defines:
- `EventLog`: minimal RPC log record used by the decoder.
- `ChunkRecord`: manifest entry used for resumability and coverage.
- `Column`: dynamic, append-only columnar buffer where *any*
   projection key becomes its own Parquet column.

Design notes
------------
- Dynamic columns are stored as strings for Arrow safety (big ints, hex).
- Base columns are strongly typed and always present.
- `extend` aligns dynamic columns by name and pads with None as needed.
- Sorting is applied on (block_number, tx_hash, log_index) before write.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

import pyarrow as pa

# === Base schema (Arrow) ===

_BASE_FIELDS: list[tuple[str, pa.DataType]] = [
    ("block_number", pa.uint64()),
    ("block_timestamp", pa.uint64()),
    ("tx_hash", pa.string()),
    ("log_index", pa.uint64()),
    ("contract", pa.string()),
    ("event", pa.string()),
]

Status = Literal["started", "done", "failed"]


# === RPC record ===


@dataclass(slots=True, frozen=True)
class EventLog:
    """Raw log as fetched from RPC, minimally normalized."""

    address: str  # lowercased 0x...
    topics: tuple[str, ...]  # lowercased 0x...
    data_hex: str  # "0x..."
    block_number: int
    tx_hash: str  # lowercased 0x...
    log_index: int
    block_timestamp: int | None = None


@dataclass(slots=True)
class Meta:
    """Lightweight metadata for a single log used during decoding."""

    block_number: int
    block_timestamp: int | None
    tx_hash: str
    log_index: int
    pool: str


# === Manifest record ===


@dataclass(slots=True)
class ChunkRecord:
    """A single chunk execution record persisted to the live manifest."""

    from_block: int
    to_block: int
    status: Status
    attempts: int
    error: str | None
    logs: int  # raw logs fetched
    decoded: int  # kept rows
    shards: int  # shard files written due to this chunk
    updated_at: float
    filtered: int = 0  # rows skipped by fast filter

    def to_json_line(self) -> str:
        """Serialize as a compact JSON line."""
        return json.dumps(asdict(self), separators=(",", ":")) + "\n"


# === Dynamic column buffer ===


@dataclass(slots=True)
class Column:
    """Dynamic columnar buffer (universal projection-first layout).

    - Base columns are always present and strongly typed.
    - Dynamic columns are created lazily upon first key appearance.
    - All dynamic values are stored as *strings* (or None) to avoid Arrow
      overflow and preserve exactness (e.g., uint256).
    """

    block_number: list[int] = field(default_factory=list)
    block_timestamp: list[int] = field(default_factory=list)
    tx_hash: list[str] = field(default_factory=list)
    log_index: list[int] = field(default_factory=list)
    contract: list[str] = field(default_factory=list)
    event: list[str] = field(default_factory=list)

    # Dynamic columns created on-demand for any projection key
    dyn: dict[str, list[str | None]] = field(default_factory=dict)
    _rows: int = 0

    @staticmethod
    def empty() -> Column:
        """Return an empty buffer."""
        return Column()

    def size(self) -> int:
        """Number of rows currently stored."""
        return self._rows

    def _append_base(self, meta: Meta, contract: str, event: str) -> None:
        """Append one row to base columns and pad existing dynamic cols."""
        self.block_number.append(meta.block_number)
        self.block_timestamp.append(int(meta.block_timestamp or 0))
        self.tx_hash.append(meta.tx_hash)
        self.log_index.append(meta.log_index)
        self.contract.append(contract)
        self.event.append(event)
        self._rows += 1
        # Pad existing dynamic columns with None for the new row
        for col in self.dyn.values():
            col.append(None)

    def _ensure_dyn_col(self, name: str) -> list[str | None]:
        """Ensure a dynamic column exists and is aligned to current row count."""
        col = self.dyn.get(name)
        if col is None:
            col = [None] * self._rows
            self.dyn[name] = col
        return col

    def append_from_parsed(
        self,
        *,
        pe_name: str,
        meta: Meta,
        values: dict[str, Any],
        contract_addr: str,
    ) -> None:
        """Append a decoded event into the buffer (all keys become columns)."""
        self._append_base(meta, contract_addr, pe_name)
        # Convert all projection values to strings for Arrow compatibility
        for k, v in values.items():
            sval: str | None = None if v is None else str(v)
            self._ensure_dyn_col(k)[-1] = sval

    def extend(self, other: Column) -> int:
        """Merge `other` into self; align dynamic columns by name."""
        n = other.size()
        if n == 0:
            return 0

        # Snapshot current row count BEFORE we extend base arrays
        old_rows = self._rows

        # 1) extend base arrays
        self.block_number.extend(other.block_number)
        self.block_timestamp.extend(other.block_timestamp)
        self.tx_hash.extend(other.tx_hash)
        self.log_index.extend(other.log_index)
        self.contract.extend(other.contract)
        self.event.extend(other.event)
        self._rows += n

        # 2) union of dynamic keys
        keys: set[str] = set(self.dyn.keys()) | set(other.dyn.keys())

        # 3) make sure each key exists and is correctly aligned
        for k in keys:
            if k not in self.dyn:
                # NEW column: fill rows that existed BEFORE this extend with None
                self.dyn[k] = [None] * old_rows

            self_col = self.dyn[k]
            ocol = other.dyn.get(k)

            if ocol is None:
                # Pad missing column with None for new rows
                self_col.extend([None] * n)
            else:
                # Append values from other buffer
                self_col.extend(ocol)

        return n

    def take_first(self, n: int) -> Column:
        """Detach and return the first `n` rows as a new buffer slice."""
        out = Column()
        out.block_number, self.block_number = self.block_number[:n], self.block_number[n:]
        out.block_timestamp, self.block_timestamp = self.block_timestamp[:n], self.block_timestamp[n:]
        out.tx_hash, self.tx_hash = self.tx_hash[:n], self.tx_hash[n:]
        out.log_index, self.log_index = self.log_index[:n], self.log_index[n:]
        out.contract, self.contract = self.contract[:n], self.contract[n:]
        out.event, self.event = self.event[:n], self.event[n:]
        for k, col in self.dyn.items():
            out.dyn[k] = col[:n]
            self.dyn[k] = col[n:]
        out._rows = n
        self._rows -= n
        return out

    def to_arrow_table(self) -> pa.Table:
        """Convert the buffer to a sorted Arrow table with deterministic schema."""
        fields = [pa.field(n, t) for n, t in _BASE_FIELDS]
        arrays: dict[str, pa.Array] = {
            "block_number": pa.array(self.block_number, type=pa.uint64()),
            "block_timestamp": pa.array(self.block_timestamp, type=pa.uint64()),
            "tx_hash": pa.array(self.tx_hash, type=pa.string()),
            "log_index": pa.array(self.log_index, type=pa.uint64()),
            "contract": pa.array(self.contract, type=pa.string()),
            "event": pa.array(self.event, type=pa.string()),
        }
        # Add dynamic columns in deterministic order
        for name in sorted(self.dyn.keys()):
            fields.append(pa.field(name, pa.string()))
            arrays[name] = pa.array(self.dyn[name], type=pa.string())
        schema = pa.schema(fields)
        return pa.Table.from_pydict(arrays, schema=schema).sort_by(
            [("block_number", "ascending"), ("tx_hash", "ascending"), ("log_index", "ascending")]
        )
