"""Block-range coverage utilities for resumable indexing.

Functions
---------
- merge_intervals: merge overlapping/adjacent [start, end] integer ranges.
- subtract_iv: subtract a set of covered intervals from a target interval.
- load_done_coverage: scan manifest files and collect 'done' ranges.

All intervals are inclusive on both ends: [start, end].
"""

from __future__ import annotations

import json
import os
from collections.abc import Generator
from pathlib import Path


def topics_fingerprint(t0s: list[str]) -> str:
    """Compact fingerprint for a set of topic0 signatures (order-insensitive)."""
    uniq = sorted({(t or "").lower() for t in t0s if t})
    return "x".join(x[:10] for x in uniq) if uniq else "none"


def iter_chunks(a: int, b: int, step: int) -> Generator[tuple[int, int], None, None]:
    """Yield inclusive [start, end] block ranges of size at most `step`."""
    x = a
    while x <= b:
        y = min(b, x + step - 1)
        yield (x, y)
        x = y + 1


def merge_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge overlapping/adjacent inclusive intervals.

    Parameters
    ----------
    intervals : list[tuple[int, int]]
        Unordered inclusive ranges.

    Returns
    -------
    list[tuple[int, int]]
        Minimal set of merged inclusive ranges.
    """
    if not intervals:
        return []
    intervals_sorted = sorted(intervals)
    out: list[list[int]] = [[intervals_sorted[0][0], intervals_sorted[0][1]]]
    for s, e in intervals_sorted[1:]:
        ms, me = out[-1]
        if s <= me + 1:
            out[-1][1] = max(me, e)
        else:
            out.append([s, e])
    return [(s, e) for s, e in out]


def subtract_iv(iv: tuple[int, int], covered: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Subtract covered inclusive intervals from a target inclusive interval.

    Parameters
    ----------
    iv : tuple[int, int]
        Target inclusive range.
    covered : list[tuple[int, int]]
        Inclusive ranges already covered.

    Returns
    -------
    list[tuple[int, int]]
        Remaining inclusive subranges not covered.
    """
    s, e = iv
    if s > e:
        return []
    if not covered:
        return [iv]
    res: list[tuple[int, int]] = []
    cur = s
    for cs, ce in covered:
        if ce < cur:
            continue
        if cs > e:
            break
        if cs > cur:
            res.append((cur, min(e, cs - 1)))
        cur = max(cur, ce + 1)
        if cur > e:
            break
    if cur <= e:
        res.append((cur, e))
    return res


def load_done_coverage(manifests_dir: Path, exclude_basename: str | None) -> list[tuple[int, int]]:
    """Load all `[from_block, to_block]` ranges with status 'done' from manifests.

    Parameters
    ----------
    manifests_dir : str
        Directory containing *.jsonl manifest files.
    exclude_basename : str | None
        If provided, skip this single file (the live manifest of the current run).

    Returns
    -------
    list[tuple[int, int]]
        Merged 'done' intervals across all manifests.
    """
    intervals: list[tuple[int, int]] = []
    if not manifests_dir.is_dir():
        raise ValueError("manifests_dir should be a directory")
    for name in os.listdir(manifests_dir):
        if not name.endswith(".jsonl"):
            continue
        if exclude_basename and name == exclude_basename:
            continue
        path = os.path.join(manifests_dir, name)
        try:
            with open(path) as f:
                for line in f:
                    if not line.strip():
                        continue
                    rec = json.loads(line)
                    if rec.get("status") == "done":
                        intervals.append((int(rec["from_block"]), int(rec["to_block"])))
        except Exception:
            # Ignore corrupt/incomplete files; resumability tolerates this.
            continue
    return merge_intervals(intervals)


def to_hex_block(block: int | str) -> str:
    """Convert block number to hex string if integer, else return as is."""
    if isinstance(block, int):
        return hex(block)
    return block


def normalize_topic0_list(topic0s: list[str]) -> list[str | None]:
    """Normalize topic0 list for RPC (None for wildcard, else hex strings)."""
    # If list is empty, it means ANY topic (wildcard) -> return [None] or [] depending on RPC?
    # Usually ["0x..."] or [null] for wildcard.
    # But here we probably want specific topics.
    return [t.lower() for t in topic0s if t]
