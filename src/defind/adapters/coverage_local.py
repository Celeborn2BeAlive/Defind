# defind/adapters/coverage_manifest.py
from __future__ import annotations

import os, json
from typing import Set

from ..ports.coverage import Coverage


def _merge(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not intervals:
        return []
    ivs = sorted(intervals)
    out: list[list[int]] = [[ivs[0][0], ivs[0][1]]]
    for s, e in ivs[1:]:
        ms, me = out[-1]
        if s <= me + 1:
            out[-1][1] = max(me, e)
        else:
            out.append([s, e])
    return [(s, e) for s, e in out]


class LocalManifestCoverage(Coverage):
    """
    Computes coverage by reading JSONL manifest files under `manifests_dir`.
    Considers only records with status == "done".
    Excludes the current run's manifest (by basename) to avoid self-counting during the run.
    """
    def __init__(self, manifests_dir: str, exclude_run_basename: str | None = None) -> None:
        self.manifests_dir = manifests_dir
        self.exclude = exclude_run_basename
        self._done: Set[tuple[int, int]] = set()
        self._load_done_chunks()

    def _load_done_chunks(self) -> None:
        if not os.path.isdir(self.manifests_dir):
            return
        for name in os.listdir(self.manifests_dir):
            if not name.endswith(".jsonl"):
                continue
            if self.exclude and name == self.exclude:
                # ignore the current run manifest to avoid counting in-flight writes
                continue
            path = os.path.join(self.manifests_dir, name)
            try:
                with open(path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        rec = json.loads(line)
                        if rec.get("status") == "done":
                            fb = int(rec["from_block"])
                            tb = int(rec["to_block"])
                            self._done.add((fb, tb))
            except Exception:
                # ignore unreadable/corrupt lines/files
                continue

    async def covered_ranges(self) -> list[tuple[int, int]]:
        return _merge(list(self._done))

    async def chunk_exists(self, from_block: int, to_block: int) -> bool:
        return (from_block, to_block) in self._done
