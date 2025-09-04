# defind/ports/rpc.py
from __future__ import annotations

from typing import Protocol, Sequence
from ..domain.models import EventLog
from ..domain.value_types import Address, Topic0


class RPCClient(Protocol):
    """Port defining the contract for an Ethereum JSON-RPC logs client."""

    async def get_logs(
        self,
        address: Address,
        topic0s: Sequence[Topic0],
        from_block: int,
        to_block: int,
    ) -> list[EventLog]:
        """Return normalized, typed logs for [from_block, to_block] inclusive."""

    async def latest_block(self) -> int:
        """Return the latest block number as an integer."""
