from __future__ import annotations
from typing import Iterable
from ..domain.models import ChunkRec

def plan_chunks(start_block: int, end_block: int, step: int) -> list[ChunkRec]:
    out: list[ChunkRec] = []
    b = start_block
    while b <= end_block:
        fb, tb = b, min(end_block, b + step - 1)
        out.append(ChunkRec(from_block=fb, to_block=tb))
        b = tb + 1
    return out

def merge_intervals(intervals: list[tuple[int,int]]) -> list[tuple[int,int]]:
    if not intervals: return []
    intervals = sorted(intervals)
    merged: list[list[int]] = [[intervals[0][0], intervals[0][1]]]
    for s, e in intervals[1:]:
        ms, me = merged[-1]
        if s <= me + 1: merged[-1][1] = max(me, e)
        else: merged.append([s, e])
    return [(s, e) for s, e in merged]

def subtract_interval(iv: tuple[int,int], covered: list[tuple[int,int]]) -> list[tuple[int,int]]:
    s, e = iv
    if s > e: return []
    if not covered: return [iv]
    res: list[tuple[int,int]] = []
    cur = s
    for cs, ce in covered:
        if ce < cur: continue
        if cs > e: break
        if cs > cur: res.append((cur, min(e, cs - 1)))
        cur = max(cur, ce + 1)
        if cur > e: break
    if cur <= e: res.append((cur, e))
    return res
