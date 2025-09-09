# fetcher.py
from __future__ import annotations

import os
import json
import time
import glob
import asyncio
from dataclasses import dataclass, asdict
from typing import Sequence, Optional, List, Tuple, Literal

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from eth_utils import to_checksum_address


# ──────────────────────────────
# Constants (topic0 filters)
# ──────────────────────────────

MINT_T0        = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
BURN_T0        = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"
COLLECT_T0     = "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0"
COLLECTFEES_T0 = "0x205860e66845f2bbc0966bfab80db9bf93fca93862ea2b9fcf6945748352b4a3"

Status = Literal["started", "done", "failed"]


# ──────────────────────────────
# Domain models
# ──────────────────────────────

@dataclass(slots=True, frozen=True)
class EventLog:
    address: str                       # lowercased hex with 0x
    topics: tuple[str, ...]            # all topics, lowercased with 0x
    data_hex: str                      # hex with 0x (or "0x")
    block_number: int
    tx_hash: str                       # lowercased hex with 0x
    log_index: int
    block_timestamp: Optional[int] = None


@dataclass(slots=True)
class DecodedRow:
    block_number: int
    block_timestamp: int
    tx_hash: str
    log_index: int
    pool: str                          # checksum address
    event: str                         # Mint | Burn | Collect | CollectFees
    owner: Optional[str]
    sender: Optional[str]
    recipient: Optional[str]
    tick_lower: int
    tick_upper: int
    liquidity: Optional[str]           # big ints as strings
    amount0: Optional[str]
    amount1: Optional[str]


@dataclass(slots=True)
class ChunkRecord:
    from_block: int
    to_block: int
    status: Status
    attempts: int
    error: Optional[str]
    logs: int              # raw logs fetched from RPC
    decoded: int           # rows kept after decoding/filtering
    shards: int            # shards written because of this chunk
    updated_at: float
    filtered: int = 0      # rows discarded by fast filter

    def to_json_line(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":")) + "\n"


# ──────────────────────────────
# Arrow schema (decoded)
# ──────────────────────────────

DECODED_SCHEMA = pa.schema([
    pa.field("block_number",    pa.int64()),
    pa.field("block_timestamp", pa.int64()),
    pa.field("tx_hash",         pa.large_string()),
    pa.field("log_index",       pa.int32()),
    pa.field("pool",            pa.large_string()),
    pa.field("event",           pa.large_string()),
    pa.field("owner",           pa.large_string()),
    pa.field("sender",          pa.large_string()),
    pa.field("recipient",       pa.large_string()),
    pa.field("tick_lower",      pa.int32()),
    pa.field("tick_upper",      pa.int32()),
    pa.field("liquidity",       pa.large_string()),
    pa.field("amount0",         pa.large_string()),
    pa.field("amount1",         pa.large_string()),
])

DECODED_COLS: tuple[str, ...] = tuple(f.name for f in DECODED_SCHEMA)


# ──────────────────────────────
# Column buffer (typed, no raw dicts)
# ──────────────────────────────

@dataclass(slots=True)
class DecodedColumns:
    block_number: List[int]
    block_timestamp: List[int]
    tx_hash: List[str]
    log_index: List[int]
    pool: List[str]
    event: List[str]
    owner: List[Optional[str]]
    sender: List[Optional[str]]
    recipient: List[Optional[str]]
    tick_lower: List[int]
    tick_upper: List[int]
    liquidity: List[Optional[str]]
    amount0: List[Optional[str]]
    amount1: List[Optional[str]]

    @classmethod
    def empty(cls) -> "DecodedColumns":
        return cls(
            block_number=[], block_timestamp=[], tx_hash=[], log_index=[], pool=[], event=[],
            owner=[], sender=[], recipient=[], tick_lower=[], tick_upper=[],
            liquidity=[], amount0=[], amount1=[]
        )

    def append(self, row: DecodedRow) -> None:
        self.block_number.append(row.block_number)
        self.block_timestamp.append(row.block_timestamp)
        self.tx_hash.append(row.tx_hash)
        self.log_index.append(row.log_index)
        self.pool.append(row.pool)
        self.event.append(row.event)
        self.owner.append(row.owner)
        self.sender.append(row.sender)
        self.recipient.append(row.recipient)
        self.tick_lower.append(row.tick_lower)
        self.tick_upper.append(row.tick_upper)
        self.liquidity.append(row.liquidity)
        self.amount0.append(row.amount0)
        self.amount1.append(row.amount1)

    def size(self) -> int:
        return len(self.tx_hash)

    def take_first(self, n: int) -> "DecodedColumns":
        """Destructively take first n rows into a new object."""
        out = DecodedColumns(
            block_number=self.block_number[:n],
            block_timestamp=self.block_timestamp[:n],
            tx_hash=self.tx_hash[:n],
            log_index=self.log_index[:n],
            pool=self.pool[:n],
            event=self.event[:n],
            owner=self.owner[:n],
            sender=self.sender[:n],
            recipient=self.recipient[:n],
            tick_lower=self.tick_lower[:n],
            tick_upper=self.tick_upper[:n],
            liquidity=self.liquidity[:n],
            amount0=self.amount0[:n],
            amount1=self.amount1[:n],
        )
        # shrink current
        self.block_number = self.block_number[n:]
        self.block_timestamp = self.block_timestamp[n:]
        self.tx_hash = self.tx_hash[n:]
        self.log_index = self.log_index[n:]
        self.pool = self.pool[n:]
        self.event = self.event[n:]
        self.owner = self.owner[n:]
        self.sender = self.sender[n:]
        self.recipient = self.recipient[n:]
        self.tick_lower = self.tick_lower[n:]
        self.tick_upper = self.tick_upper[n:]
        self.liquidity = self.liquidity[n:]
        self.amount0 = self.amount0[n:]
        self.amount1 = self.amount1[n:]
        return out

    def to_arrow_table(self) -> pa.Table:
        arrays = {
            "block_number":    pa.array(self.block_number,    type=DECODED_SCHEMA.field("block_number").type),
            "block_timestamp": pa.array(self.block_timestamp, type=DECODED_SCHEMA.field("block_timestamp").type),
            "tx_hash":         pa.array(self.tx_hash,         type=DECODED_SCHEMA.field("tx_hash").type),
            "log_index":       pa.array(self.log_index,       type=DECODED_SCHEMA.field("log_index").type),
            "pool":            pa.array(self.pool,            type=DECODED_SCHEMA.field("pool").type),
            "event":           pa.array(self.event,           type=DECODED_SCHEMA.field("event").type),
            "owner":           pa.array(self.owner,           type=DECODED_SCHEMA.field("owner").type),
            "sender":          pa.array(self.sender,          type=DECODED_SCHEMA.field("sender").type),
            "recipient":       pa.array(self.recipient,       type=DECODED_SCHEMA.field("recipient").type),
            "tick_lower":      pa.array(self.tick_lower,      type=DECODED_SCHEMA.field("tick_lower").type),
            "tick_upper":      pa.array(self.tick_upper,      type=DECODED_SCHEMA.field("tick_upper").type),
            "liquidity":       pa.array(self.liquidity,       type=DECODED_SCHEMA.field("liquidity").type),
            "amount0":         pa.array(self.amount0,         type=DECODED_SCHEMA.field("amount0").type),
            "amount1":         pa.array(self.amount1,         type=DECODED_SCHEMA.field("amount1").type),
        }
        return pa.Table.from_pydict(arrays, schema=DECODED_SCHEMA).sort_by([
            ("block_number", "ascending"),
            ("tx_hash", "ascending"),
            ("log_index", "ascending"),
        ])


# ──────────────────────────────
# RPC client (typed)
# ──────────────────────────────

def _to_hex_block(x: int) -> str: return hex(x)
def _topics_param(topic0s: Sequence[str]) -> list[list[str]]: return [[t.lower() for t in topic0s]]

class RPC:
    def __init__(self, url: str, *, timeout_s: int = 20, max_connections: int = 64) -> None:
        self.url = url
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=timeout_s, read=timeout_s, write=timeout_s, pool=max(30, timeout_s*3)),
            limits=httpx.Limits(max_connections=max_connections, max_keepalive_connections=max(1, max_connections//2)),
            http2=True,
        )

    async def latest_block(self) -> int:
        payload = {"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}
        r = await self.client.post(self.url, json=payload); r.raise_for_status()
        return int(r.json()["result"], 16)

    async def get_logs(self, *, address: str, topic0s: Sequence[str], from_block: int, to_block: int) -> list[EventLog]:
        params = [{"address":address.lower(),"fromBlock":_to_hex_block(from_block),
                   "toBlock":_to_hex_block(to_block),"topics":_topics_param(topic0s)}]
        r = await self.client.post(self.url, json={"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":params})
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            e = data["error"]; raise RuntimeError(f"RPC error: {e.get('code')} {e.get('message')}")
        out: list[EventLog] = []
        for rl in data.get("result", []):
            topics = tuple((t if isinstance(t,str) else t.decode()).lower() for t in rl.get("topics", []))
            ts = rl.get("blockTimestamp")
            ts_i = int(ts,16) if isinstance(ts,str) and ts.startswith("0x") else (int(ts) if isinstance(ts,int) else None)
            out.append(EventLog(
                address=rl["address"].lower(),
                topics=topics,
                data_hex=str(rl.get("data") or "0x"),
                block_number=int(rl["blockNumber"],16),
                tx_hash=(rl.get("transactionHash") or rl.get("transaction_hash") or "").lower(),
                log_index=int(rl["logIndex"],16),
                block_timestamp=ts_i,
            ))
        return out

    async def aclose(self) -> None:
        await self.client.aclose()


# ──────────────────────────────
# Decoder (fast zero short-circuit + filtering)
# ──────────────────────────────

@dataclass(slots=True, frozen=True)
class Meta:
    block_number: int
    block_timestamp: Optional[int]
    tx_hash: str
    log_index: int
    pool: str

def _word(b: bytes, i: int) -> bytes: o = i*32; return b[o:o+32]
def _addr_from_word(w: bytes) -> str: return to_checksum_address("0x"+w[-20:].hex())

def _int24_from_topic_hex(t: str) -> int:
    h = t[2:] if t[:2].lower()=="0x" else t
    b = bytes.fromhex(h[-6:])
    v = int.from_bytes(b, "big")
    return v - (1<<24) if (v & (1<<23)) else v

def decode_event(*, topics: Sequence[str], data: bytes, meta: Meta) -> Optional[DecodedRow]:
    if not topics:
        return None

    t0 = topics[0].lower()
    top = [(t[2:] if t[:2].lower() == "0x" else t).lower() for t in topics]

    event: Optional[str] = None
    owner = sender = recipient = None
    tl = tu = None
    liq_s = amt0_s = amt1_s = None

    # MINT: ["address","uint128","uint256","uint256"]
    if t0 == MINT_T0 and len(top) >= 4 and len(data) >= 32 * 4:
        w1 = _word(data, 1)  # liquidity
        w2 = _word(data, 2)  # amount0
        w3 = _word(data, 3)  # amount1
        # fast zero check
        if not (any(w1) or any(w2) or any(w3)):
            return None

        event = "Mint"
        owner = to_checksum_address("0x" + top[1][-40:])
        tl = _int24_from_topic_hex(top[2]); tu = _int24_from_topic_hex(top[3])
        sender = _addr_from_word(_word(data, 0))
        liquidity_i = int.from_bytes(w1, "big")
        amount0_i   = int.from_bytes(w2, "big")
        amount1_i   = int.from_bytes(w3, "big")
        if liquidity_i == 0 and amount0_i == 0 and amount1_i == 0:
            return None
        liq_s, amt0_s, amt1_s = str(liquidity_i), str(amount0_i), str(amount1_i)

    # BURN: ["uint128","uint256","uint256"]
    elif t0 == BURN_T0 and len(top) >= 4 and len(data) >= 32 * 3:
        w0 = _word(data, 0)  # liquidity
        w1 = _word(data, 1)  # amount0
        w2 = _word(data, 2)  # amount1
        if not (any(w0) or any(w1) or any(w2)):
            return None

        event = "Burn"
        owner = to_checksum_address("0x" + top[1][-40:])
        tl = _int24_from_topic_hex(top[2]); tu = _int24_from_topic_hex(top[3])
        liquidity_i = int.from_bytes(w0, "big")
        amount0_i   = int.from_bytes(w1, "big")
        amount1_i   = int.from_bytes(w2, "big")
        if liquidity_i == 0 and amount0_i == 0 and amount1_i == 0:
            return None
        liq_s, amt0_s, amt1_s = str(liquidity_i), str(amount0_i), str(amount1_i)

    # COLLECT: ["address","uint128","uint128"]
    elif t0 == COLLECT_T0 and len(top) >= 4 and len(data) >= 32 * 3:
        w1 = _word(data, 1)  # amount0
        w2 = _word(data, 2)  # amount1
        if not (any(w1) or any(w2)):
            return None

        event = "Collect"
        owner = to_checksum_address("0x" + top[1][-40:])
        tl = _int24_from_topic_hex(top[2]); tu = _int24_from_topic_hex(top[3])
        recipient = _addr_from_word(_word(data, 0))
        amount0_i = int.from_bytes(w1, "big")
        amount1_i = int.from_bytes(w2, "big")
        if amount0_i == 0 and amount1_i == 0:
            return None
        amt0_s, amt1_s = str(amount0_i), str(amount1_i)

    # COLLECT FEES: ["uint128","uint128"]
    elif t0 == COLLECTFEES_T0 and len(top) >= 2 and len(data) >= 32 * 2:
        w0 = _word(data, 0)  # amount0
        w1 = _word(data, 1)  # amount1
        if not (any(w0) or any(w1)):
            return None

        event = "CollectFees"
        recipient = to_checksum_address("0x" + top[1][-40:])
        amount0_i = int.from_bytes(w0, "big")
        amount1_i = int.from_bytes(w1, "big")
        if amount0_i == 0 and amount1_i == 0:
            return None
        amt0_s, amt1_s = str(amount0_i), str(amount1_i)

    # Not one of our events / not enough data
    if event is None:
        return None

    return DecodedRow(
        block_number   = meta.block_number,
        block_timestamp= meta.block_timestamp or 0,
        tx_hash        = meta.tx_hash,
        log_index      = meta.log_index,
        pool           = to_checksum_address(meta.pool),
        event          = event,
        owner          = owner,
        sender         = sender,
        recipient      = recipient,
        tick_lower     = tl or 0,
        tick_upper     = tu or 0,
        liquidity      = liq_s,
        amount0        = amt0_s,
        amount1        = amt1_s,
    )


# ──────────────────────────────
# Manifest (live JSONL)
# ──────────────────────────────

class LiveManifest:
    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        open(self.path, "a").close()
        self._lock = asyncio.Lock()

    async def append(self, rec: ChunkRecord) -> None:
        line = rec.to_json_line()
        async with self._lock:
            await asyncio.to_thread(self._write_line, self.path, line)

    @staticmethod
    def _write_line(path: str, line: str) -> None:
        with open(path, "a", buffering=1) as f:
            f.write(line); f.flush(); os.fsync(f.fileno())


# ──────────────────────────────
# Coverage helpers (from manifests)
# ──────────────────────────────

def _merge_intervals(intervals: List[Tuple[int,int]]) -> List[Tuple[int,int]]:
    if not intervals: return []
    ivs = sorted(intervals); out: List[List[int]] = [[ivs[0][0], ivs[0][1]]]
    for s,e in ivs[1:]:
        ms,me = out[-1]
        if s <= me+1: out[-1][1] = max(me,e)
        else: out.append([s,e])
    return [(s,e) for s,e in out]

def _subtract_iv(iv: Tuple[int,int], covered: List[Tuple[int,int]]) -> List[Tuple[int,int]]:
    s,e = iv
    if s>e: return []
    if not covered: return [iv]
    res: List[Tuple[int,int]] = []; cur = s
    for cs,ce in covered:
        if ce<cur: continue
        if cs>e: break
        if cs>cur: res.append((cur, min(e, cs-1)))
        cur = max(cur, ce+1)
        if cur>e: break
    if cur<=e: res.append((cur,e))
    return res

def load_done_coverage(manifests_dir: str, exclude_basename: Optional[str]) -> List[Tuple[int,int]]:
    ivs: List[Tuple[int,int]] = []
    if not os.path.isdir(manifests_dir): return []
    for name in os.listdir(manifests_dir):
        if not name.endswith(".jsonl"): continue
        if exclude_basename and name == exclude_basename: continue
        path = os.path.join(manifests_dir, name)
        try:
            with open(path, "r") as f:
                for line in f:
                    if not line.strip(): continue
                    rec = json.loads(line)
                    if rec.get("status") == "done":
                        ivs.append((int(rec["from_block"]), int(rec["to_block"])))
        except Exception:
            continue
    return _merge_intervals(ivs)


# ──────────────────────────────
# Shard aggregator (strict 250k good rows; final partial optional)
# ──────────────────────────────

class ShardAggregator:
    def __init__(
        self,
        out_root: str,
        addr_slug: str,
        topics_fp: str,
        *,
        rows_per_shard: int = 250_000,
        codec: str = "zstd",
        write_final_partial: bool = True,  # set False to suppress the last partial file
    ) -> None:
        self.rows_per_shard = rows_per_shard
        self.codec = codec
        self.write_final_partial = write_final_partial
        self.key_dir = os.path.join(out_root, f"{addr_slug}__topics-{topics_fp}")
        self.shards_dir = os.path.join(self.key_dir, "shards")
        os.makedirs(self.shards_dir, exist_ok=True)
        self.buf = DecodedColumns.empty()
        self.buffered = 0
        self.shard_idx = self._next_shard_index()

    def _next_shard_index(self) -> int:
        existing = sorted(glob.glob(os.path.join(self.shards_dir, "shard_*.parquet")))
        return 1 if not existing else int(os.path.basename(existing[-1]).split("_")[1].split(".")[0]) + 1

    def _write_table(self, cols: DecodedColumns, shard_idx: int) -> str:
        table = cols.to_arrow_table()
        out_path = os.path.join(self.shards_dir, f"shard_{shard_idx:05d}.parquet")
        pq.write_table(table, out_path, compression=self.codec)
        print(f"💾 wrote shard {shard_idx:05d} → {out_path}  (rows={len(table)})")
        return out_path

    def add(self, cols: DecodedColumns) -> List[str]:
        """Append decoded rows and flush strictly full shards (250k good rows)."""
        n = cols.size()
        if n == 0: return []
        # append
        for row_idx in range(n):
            self.buf.block_number.append(cols.block_number[row_idx])
            self.buf.block_timestamp.append(cols.block_timestamp[row_idx])
            self.buf.tx_hash.append(cols.tx_hash[row_idx])
            self.buf.log_index.append(cols.log_index[row_idx])
            self.buf.pool.append(cols.pool[row_idx])
            self.buf.event.append(cols.event[row_idx])
            self.buf.owner.append(cols.owner[row_idx])
            self.buf.sender.append(cols.sender[row_idx])
            self.buf.recipient.append(cols.recipient[row_idx])
            self.buf.tick_lower.append(cols.tick_lower[row_idx])
            self.buf.tick_upper.append(cols.tick_upper[row_idx])
            self.buf.liquidity.append(cols.liquidity[row_idx])
            self.buf.amount0.append(cols.amount0[row_idx])
            self.buf.amount1.append(cols.amount1[row_idx])
        self.buffered += n

        written: List[str] = []
        while self.buffered >= self.rows_per_shard:
            slice_cols = self.buf.take_first(self.rows_per_shard)   # exactly 250k good rows
            out_path = self._write_table(slice_cols, self.shard_idx)
            written.append(out_path)
            self.shard_idx += 1
            self.buffered -= self.rows_per_shard
        return written

    def close(self) -> Optional[str]:
        """
        Flush the remainder:
          - if write_final_partial=True (default), write it (avoids data loss).
          - if False, skip writing if < rows_per_shard.
        """
        if self.buffered == 0:
            return None
        if not self.write_final_partial and self.buffered < self.rows_per_shard:
            # do not write the tail; keep memory cleanup
            self.buf = DecodedColumns.empty()
            self.buffered = 0
            return None
        remaining = self.buf.take_first(self.buffered)
        out_path = self._write_table(remaining, self.shard_idx)
        self.shard_idx += 1
        self.buffered = 0
        return out_path


# ──────────────────────────────
# Orchestrator (public entry)
# ──────────────────────────────

async def fetch_decode_streaming_to_shards(
    *,
    rpc_url: str,
    address: str,
    topic0s: list[str],
    start_block: int | str,          # int | "earliest"/"genesis"
    end_block: int | str,            # int | "latest"
    step: int,
    concurrency: int,
    out_root: str,                   # base dir (manifests + shards)
    rows_per_shard: int = 250_000,
    batch_decode_rows: int = 10_000,
    timeout_s: int = 20,
    min_split_span: int = 2_000,
    write_final_partial: bool = True,  # set False if you truly want ONLY 250k-row files
) -> dict[str, int]:
    """
    Streaming pipeline:
      RPC logs → decode on the fly → aggregate → write shard every `rows_per_shard`.
    Coverage is computed from previous manifests in the same key_dir.
    """
    addr_slug = address.lower()
    topics_fp = _topics_fingerprint(topic0s)

    key_dir = os.path.join(out_root, f"{addr_slug}__topics-{topics_fp}")
    man_dir = os.path.join(key_dir, "manifests")
    os.makedirs(man_dir, exist_ok=True)

    rpc = RPC(rpc_url, timeout_s=timeout_s, max_connections=max(32, 2*concurrency))
    try:
        s_block = 0 if (isinstance(start_block, str) and start_block.lower() in ("earliest", "genesis")) else int(start_block)
        e_block = await rpc.latest_block() if (isinstance(end_block, str) and end_block.lower() == "latest") else int(end_block)
        if s_block > e_block:
            raise ValueError(f"start_block ({s_block}) must be <= end_block ({e_block})")

        run_basename = f"run_{_now_ts()}_{addr_slug}_{topics_fp}_{s_block}_{e_block}.jsonl"
        manifest = LiveManifest(os.path.join(man_dir, run_basename))
        covered = load_done_coverage(man_dir, exclude_basename=run_basename)

        # plan seeds
        seeds: list[tuple[int,int]] = []
        for us, ue in _subtract_iv((s_block, e_block), covered):
            b = us
            while b <= ue:
                fb, tb = b, min(ue, b + step - 1)
                seeds.append((fb, tb)); b = tb + 1
        if not seeds:
            return {"processed_ok":0,"processed_failed":0,"executed_subranges":0,
                    "total_logs":0,"partially_covered_split":0,"shards_written":0}

        agg = ShardAggregator(
            out_root, addr_slug, topics_fp,
            rows_per_shard=rows_per_shard,
            write_final_partial=write_final_partial,  # default True to avoid data loss
        )
        agg_lock = asyncio.Lock()
        sem = asyncio.Semaphore(concurrency)

        stats = {"processed_ok":0,"processed_failed":0,"executed_subranges":0,
                 "total_logs":0,"partially_covered_split":0,"shards_written":0}

        async def process_interval(fb:int, tb:int) -> None:
            stack: list[tuple[int,int]] = [(fb, tb)]
            while stack:
                a, b = stack.pop()
                await manifest.append(ChunkRecord(a,b,"started",0,None,0,0,0,time.time(),0))
                try:
                    async with sem:
                        logs = await rpc.get_logs(address=address, topic0s=topic0s, from_block=a, to_block=b)
                    stats["executed_subranges"] += 1
                    stats["total_logs"] += len(logs)

                    cols = DecodedColumns.empty()
                    batch_count = 0
                    decoded_rows = 0
                    filtered_rows = 0
                    shards_here = 0

                    for ev in logs:
                        topics = ev.topics
                        if not topics:
                            filtered_rows += 1
                            continue
                        data_hex = ev.data_hex[2:] if ev.data_hex.lower().startswith("0x") else ev.data_hex
                        data_bytes = bytes.fromhex(data_hex) if data_hex else b""
                        meta = Meta(ev.block_number, ev.block_timestamp, ev.tx_hash, ev.log_index, ev.address)

                        row = decode_event(topics=topics, data=data_bytes, meta=meta)
                        if row is None:
                            filtered_rows += 1
                            continue

                        cols.append(row)
                        batch_count += 1
                        decoded_rows += 1

                        if batch_count >= batch_decode_rows:
                            async with agg_lock:
                                written = agg.add(cols)   # writes only full shards (250k good rows)
                            shards_here += len(written)
                            cols = DecodedColumns.empty()
                            batch_count = 0

                    if batch_count > 0:
                        async with agg_lock:
                            written = agg.add(cols)
                        shards_here += len(written)

                    stats["processed_ok"] += 1
                    stats["shards_written"] += shards_here
                    await manifest.append(ChunkRecord(a,b,"done",1,None,len(logs),decoded_rows,shards_here,time.time(),filtered_rows))

                except Exception as e:
                    span = b - a + 1
                    if span > min_split_span:
                        mid = (a + b)//2
                        left, right = (a, mid), (mid+1, b)
                        stats["partially_covered_split"] += 1
                        if left[0] <= left[1]:  stack.append(left)
                        if right[0] <= right[1]: stack.append(right)
                    else:
                        stats["processed_failed"] += 1
                        await manifest.append(ChunkRecord(a,b,"failed",1,str(e),0,0,0,time.time(),0))

        await asyncio.gather(*(asyncio.create_task(process_interval(fb, tb)) for fb, tb in seeds))

        async with agg_lock:
            last = agg.close()  # writes the final partial shard iff write_final_partial=True
        if last: stats["shards_written"] += 1

        return stats

    finally:
        await rpc.aclose()


# ──────────────────────────────
# Utils
# ──────────────────────────────

def _now_ts() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _topics_fingerprint(topic0s: Sequence[str]) -> str:
    base = ",".join(sorted([t.lower() for t in topic0s]))
    import hashlib as h
    return h.sha1(base.encode()).hexdigest()[:10]
