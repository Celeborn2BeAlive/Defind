from __future__ import annotations
from dataclasses import dataclass
from typing import Literal
from .value_types import Address, EventKind, Topic0, Status

@dataclass(slots=True, frozen=True)
class BlockRange:
    start: int
    end: int
    def span(self) -> int: return self.end - self.start + 1

@dataclass(slots=True, frozen=True)
class EventLog:
    address: Address
    topic0: Topic0
    data_hex: str
    block_number: int
    tx_hash: str
    log_index: int

@dataclass(slots=True, frozen=True)
class ChunkRec:
    from_block: int
    to_block: int
    status: Status = "pending"
    attempts: int = 0
    error: str | None = None
    logs: int = 0
    updated_at: float = 0.0

@dataclass(slots=True, frozen=True)
class DecodedRow:
    block_number: int
    block_timestamp: int | None
    tx_hash: str
    log_index: int
    pool: str
    event: EventKind
    owner: str | None
    sender: str | None
    recipient: str | None
    tick_lower: int | None
    tick_upper: int | None
    liquidity: str | None      # big ints as strings
    amount0: str | None
    amount1: str | None
