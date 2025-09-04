from __future__ import annotations
import asyncio, httpx
from typing import Sequence
from ..domain.models import EventLog
from ..domain.value_types import Address, Topic0
from ..ports.rpc import RPCClient

def _to_hex_block(n: int) -> str: return hex(int(n))
def _is_topic_hash(x: str) -> bool: return isinstance(x, str) and x.startswith("0x") and len(x)==66
def _normalize_topic0_list(t0s: Sequence[Topic0]) -> list[str]:
    out: list[str] = []
    for t in t0s:
        s = str(t).strip().lower()
        out.append(s)
    return out

def _build_topics_param(topic0s: Sequence[Topic0]) -> list[list[str]]:
    t0s = _normalize_topic0_list(topic0s)
    if not all(_is_topic_hash(x) for x in t0s):
        raise ValueError(f"Invalid topic0(s): {t0s}")
    return [t0s]

class HttpxRPC(RPCClient):
    def __init__(self, rpc_url: str, timeout_s: int = 20, max_conn: int = 64) -> None:
        self.rpc_url = rpc_url
        self.client = httpx.AsyncClient(
            http2=True,
            timeout=httpx.Timeout(timeout_s),
            limits=httpx.Limits(max_connections=max_conn, max_keepalive_connections=max_conn//2),
        )

    async def latest_block(self) -> int:
        payload = {"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}
        r = await self.client.post(self.rpc_url, json=payload)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            msg = data["error"].get("message") if isinstance(data["error"], dict) else str(data["error"])
            raise RuntimeError(f"eth_blockNumber RPC error: {msg}")
        return int(data["result"], 16)

    async def get_logs(self, address: Address, topic0s: Sequence[Topic0], from_block: int, to_block: int) -> list[EventLog]:
        payload = {"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{
            "address": str(address),
            "fromBlock": _to_hex_block(from_block),
            "toBlock": _to_hex_block(to_block),
            "topics": _build_topics_param(topic0s),
        }]}
        # retry on 429 with simple backoff (mirror of your logic)
        for attempt in range(3):
            r = await self.client.post(self.rpc_url, json=payload)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                delay = max(1.0, float(ra)) if ra and ra.isdigit() else (1.0 * (2**attempt))
                await asyncio.sleep(delay); continue
            r.raise_for_status()
            data = r.json()
            if "error" in data:
                err = data["error"]; code = err.get("code"); msg = err.get("message")
                raise RuntimeError(f"RPC error code={code} message={msg}")
            res = data.get("result", [])
            typed: list[EventLog] = []
            for rl in res:
                topics = rl.get("topics", [])
                t0 = topics[0] if topics else "0x" + ("0"*64)
                typed.append(EventLog(
                    address=Address(rl["address"].lower()),
                    topic0=Topic0(t0.lower()),
                    data_hex=rl["data"],
                    block_number=int(rl["blockNumber"], 16),
                    tx_hash=rl["transactionHash"].lower(),
                    log_index=int(rl["logIndex"], 16),
                ))
            return typed
        raise RuntimeError("Retries exhausted for eth_getLogs")
