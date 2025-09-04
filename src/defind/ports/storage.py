# defind/ports/storage.py
from __future__ import annotations

from typing import Iterable, Protocol
from ..domain.models import EventLog, ChunkRec


class EventSink(Protocol):
    """Port for writing a single chunk of events to durable storage (e.g., Parquet)."""

    async def write_chunk(
        self,
        from_block: int,
        to_block: int,
        events: Iterable[EventLog],
    ) -> None:
        """Persist the events belonging to the chunk [from_block, to_block]."""


class ManifestSink(Protocol):
    """Port for appending run/chunk status records (e.g., JSONL manifest)."""

    async def append(self, rec: ChunkRec) -> None:
        """Append a manifest record atomically (callers handle ordering/locking)."""
