from __future__ import annotations
import os, json, asyncio, time
from dataclasses import asdict
from ..ports.storage import ManifestSink
from ..domain.models import ChunkRec

class JSONLManifest(ManifestSink):
    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._lock = asyncio.Lock()

    async def append(self, rec: ChunkRec) -> None:
        line = json.dumps(asdict(rec), separators=(",", ":")) + "\n"
        async with self._lock:
            with open(self.path, "a") as f:
                f.write(line); f.flush(); os.fsync(f.fileno())
