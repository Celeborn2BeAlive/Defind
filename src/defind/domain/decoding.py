from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
from eth_utils import to_checksum_address

from defind.domain.value_types import EventKind


# Topic0 constants (lowercase, NO "0x") — keep close to your notebook values
MINT_T0        = "7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
BURN_T0        = "0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"
COLLECT_T0     = "70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0"
COLLECTFEES_T0 = "205860e66845f2bbc0966bfab80db9bf93fca93862ea2b9fcf6945748352b4a3"

# ---------- vector helpers (Arrow-first; fall back to Python when needed) -----

def _hex_to_int64(col: pa.Array) -> list[int]:
    """Handles 0x..., decimal strings, and native ints; None -> 0 (match notebook)."""
    out: list[int] = []
    for v in col.to_pylist():
        if v is None:
            out.append(0)
        elif isinstance(v, int):
            out.append(v)
        else:
            s = v.lower()
            out.append(int(s, 16) if s.startswith("0x") else int(s))
    return out

def _hex_to_0x_lower(col: pa.Array) -> list[str | None]:
    """Normalize to lowercase hex with '0x' or None."""
    out: list[str | None] = []
    for v in col.to_pylist():
        if v is None:
            out.append(None)
        else:
            s = v if isinstance(v, str) else v.decode()
            s = s.lower()
            out.append(s if s.startswith("0x") else "0x" + s)
    return out

def _addr_checksum(col: pa.Array) -> list[str | None]:
    out: list[str | None] = []
    for v in col.to_pylist():
        if v is None:
            out.append(None)
            continue
        s = v if isinstance(v, str) else v.decode()
        s = s.lower()
        if not s.startswith("0x"):
            s = "0x" + s
        s = "0x" + s[-40:]  # last 20 bytes
        out.append(to_checksum_address(s))
    return out

def _hexstr_to_bytes(col: pa.Array) -> list[bytes]:
    out: list[bytes] = []
    for v in col.to_pylist():
        if v is None:
            out.append(b""); continue
        s = v if isinstance(v, str) else v.decode()
        h = s[2:] if s[:2].lower() == "0x" else s
        if len(h) % 2: h = "0" + h
        out.append(bytes.fromhex(h) if h else b"")
    return out

def _topics_list(col: pa.Array) -> list[list[str]]:
    """Lowercase, NO '0x' for every topic in each row."""
    out: list[list[str]] = []
    for row in col.to_pylist():
        if not row:
            out.append([]); continue
        out.append([(t[2:] if t[:2].lower() == "0x" else t).lower() for t in row])
    return out

def _int24_from_topic_hex(t_no0x: str) -> int:
    """Signed int24 from topic hex (string without 0x)."""
    b = bytes.fromhex(t_no0x[-6:])
    v = int.from_bytes(b, "big")
    return v - (1 << 24) if (v & (1 << 23)) else v

# --------- 32B word slicing (fast, no eth_abi) --------------------------------
def _word(b: bytes, i: int) -> bytes:
    return b[i*32:(i+1)*32]

def _u256(w: bytes) -> int:
    return int.from_bytes(w, "big")

def _u128(w: bytes) -> int:
    return int.from_bytes(w[-16:], "big")

def _addr_from_word(w: bytes) -> str:
    return to_checksum_address("0x" + w[-20:].hex())

def _decode_mint(data_b: bytes) -> tuple[str, int, int, int]:
    # ["address","uint128","uint256","uint256"]
    return (_addr_from_word(_word(data_b, 0)),
            _u128(_word(data_b, 1)),
            _u256(_word(data_b, 2)),
            _u256(_word(data_b, 3)))

def _decode_burn(data_b: bytes) -> tuple[int, int, int]:
    # ["uint128","uint256","uint256"]
    return (_u128(_word(data_b, 0)),
            _u256(_word(data_b, 1)),
            _u256(_word(data_b, 2)))

def _decode_collect(data_b: bytes) -> tuple[str, int, int]:
    # ["address","uint128","uint128"]
    return (_addr_from_word(_word(data_b, 0)),
            _u128(_word(data_b, 1)),
            _u128(_word(data_b, 2)))

def _decode_collectfees(data_b: bytes) -> tuple[int, int]:
    # ["uint128","uint128"]
    return (_u128(_word(data_b, 0)),
            _u128(_word(data_b, 1)))

# ---------------------------- public API --------------------------------------

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

COLS = [f.name for f in DECODED_SCHEMA]

def empty_columns() -> dict[str, list]:
    return {name: [] for name in COLS}

def decode_events_table(raw_table: pa.Table) -> dict[str, list]:
    """
    Accepts a *raw logs* Parquet table (as written by your fetcher) and returns
    column lists matching DECODED_SCHEMA. If nothing decodes, returns {}.
    """
    table = raw_table.combine_chunks()
    if len(table) == 0:
        return {}

    # Required / optional input columns (match your fetcher output) :contentReference[oaicite:3]{index=3}
    bn     = _hex_to_int64(table["blockNumber"])
    txh    = _hex_to_0x_lower(table["transactionHash"])
    addr   = _addr_checksum(table["address"])
    data_b = _hexstr_to_bytes(table["data"])
    ts     = _hex_to_int64(table["blockTimestamp"]) if "blockTimestamp" in table.column_names else [None]*len(bn)
    topics = _topics_list(table["topics"])
    logi   = _hex_to_int64(table["logIndex"]) if "logIndex" in table.column_names else [0]*len(bn)

    out = empty_columns()
    n = len(bn)

    for i in range(n):
        tlist = topics[i]
        if not tlist:
            continue
        t0 = tlist[0]
        if t0 not in (MINT_T0, BURN_T0, COLLECT_T0, COLLECTFEES_T0):
            continue

        db = data_b[i]
        if len(db) < 32:
            continue

        event: EventKind | None = None
        owner = sender = recipient = None
        tl = tu = None
        liq_s = amt0_s = amt1_s = None

        try:
            if t0 == MINT_T0 and len(tlist) >= 4:
                event = "Mint"
                owner = to_checksum_address("0x" + tlist[1][-40:])
                tl = _int24_from_topic_hex(tlist[2])
                tu = _int24_from_topic_hex(tlist[3])
                sender_, amount, amount0, amount1 = _decode_mint(db)
                sender = sender_
                liq_s  = str(int(amount))
                amt0_s = str(int(amount0))
                amt1_s = str(int(amount1))

            elif t0 == BURN_T0 and len(tlist) >= 4:
                event = "Burn"
                owner = to_checksum_address("0x" + tlist[1][-40:])
                tl = _int24_from_topic_hex(tlist[2])
                tu = _int24_from_topic_hex(tlist[3])
                amount, amount0, amount1 = _decode_burn(db)
                liq_s  = str(int(amount))
                amt0_s = str(int(amount0))
                amt1_s = str(int(amount1))

            elif t0 == COLLECT_T0 and len(tlist) >= 4:
                event = "Collect"
                owner = to_checksum_address("0x" + tlist[1][-40:])
                tl = _int24_from_topic_hex(tlist[2])
                tu = _int24_from_topic_hex(tlist[3])
                recipient_, amount0, amount1 = _decode_collect(db)
                recipient = recipient_
                amt0_s = str(int(amount0))
                amt1_s = str(int(amount1))

            elif t0 == COLLECTFEES_T0 and len(tlist) >= 2:
                event = "CollectFees"
                recipient = to_checksum_address("0x" + tlist[1][-40:])
                amount0, amount1 = _decode_collectfees(db)
                amt0_s = str(int(amount0))
                amt1_s = str(int(amount1))

            if event is None:
                continue

            out["block_number"].append(bn[i])
            out["block_timestamp"].append(ts[i])
            out["tx_hash"].append(txh[i])
            out["log_index"].append(logi[i])
            out["pool"].append(addr[i])
            out["event"].append(event)
            out["owner"].append(owner)
            out["sender"].append(sender)
            out["recipient"].append(recipient)
            out["tick_lower"].append(tl)
            out["tick_upper"].append(tu)
            out["liquidity"].append(liq_s)
            out["amount0"].append(amt0_s)
            out["amount1"].append(amt1_s)

        except Exception:
            # Skip bad row (mirror notebook behavior)
            continue

    return out if out["tx_hash"] else {}
