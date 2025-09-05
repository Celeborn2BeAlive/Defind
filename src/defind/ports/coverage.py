# defind/ports/coverage.py
from __future__ import annotations
from typing import Protocol

class Coverage(Protocol):
    async def covered_ranges(self) -> list[tuple[int, int]]:
        """Return merged, inclusive [from,to] ranges already persisted for the current addr/topics."""

    async def chunk_exists(self, from_block: int, to_block: int) -> bool:
        """Return True if the specific chunk object/file already exists."""
