from __future__ import annotations

import asyncio
import os
from pathlib import Path

from defind.core.models import ChunkRecord


class LiveManifest:
    """Thread-safe manifest writer for tracking chunk processing status.

    Provides atomic append operations for chunk records with proper file locking.
    """

    def __init__(self, path: Path) -> None:
        """Initialize manifest at the given path.

        Args:
            path: File path for the manifest JSONL file
        """
        self.path = path
        path.parent.mkdir(exist_ok=True, parents=True)
        open(self.path, "a").close()
        self._lock = asyncio.Lock()

    async def append(self, rec: ChunkRecord) -> None:
        """Append a chunk record to the manifest atomically.

        Args:
            rec: ChunkRecord to write to the manifest
        """
        line = rec.to_json_line()
        async with self._lock:
            await asyncio.to_thread(self._write_line, self.path, line)

    @staticmethod
    def _write_line(path: Path, line: str) -> None:
        """Write a line to file with immediate flush and sync."""
        with open(path, "a", buffering=1) as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())
