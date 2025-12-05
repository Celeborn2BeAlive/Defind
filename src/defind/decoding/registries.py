"""Concrete registries grouped by domain (CL pool, Gauge, VFAT).

These factory functions return *composable* registries. You can merge them
with `{**a, **b}` or via `add_many` if you want a single combined registry.

Example
-------
>>> from defind.decoding.registries import make_clpool_registry, make_gauge_registry
>>> reg = {**make_clpool_registry(), **make_gauge_registry()}
"""

from __future__ import annotations

from defind.core.constants import (
    BURN_T0,
    CLAIM_REWARDS_T0,
    COLLECT_T0,
    COLLECTFEES_T0,
    DEPLOY_T0,
    DEPOSIT_T0,
    MINT_T0,
    WITHDRAW_T0,
)

from .registry import add_event_spec
from .specs import DataFieldSpec, EventRegistry, EventSpec, ProjectionRefs, TopicFieldSpec

# -------------------------
# CL Pool registry (Mint/Burn/Collect/CollectFees)
# -------------------------


def make_clpool_registry() -> EventRegistry:
    """Return registry for concentrated-liquidity pool events."""
    reg: EventRegistry = {}

    # Mint
    add_event_spec(
        reg,
        EventSpec(
            topic0=MINT_T0,
            name="Mint",
            topic_fields=[
                TopicFieldSpec("owner", 1, "address"),
                TopicFieldSpec("tick_lower", 2, "int24"),
                TopicFieldSpec("tick_upper", 3, "int24"),
            ],
            data_fields=[
                DataFieldSpec("sender", 0, "address"),
                DataFieldSpec("liquidity", 1, "uint128"),
                DataFieldSpec("amount0", 2, "uint256"),
                DataFieldSpec("amount1", 3, "uint256"),
            ],
            projection={
                "owner": ProjectionRefs.TopicRef(name="owner"),
                "sender": ProjectionRefs.DataRef(name="sender"),
                "tick_lower": ProjectionRefs.TopicRef(name="tick_lower"),
                "tick_upper": ProjectionRefs.TopicRef(name="tick_upper"),
                "liquidity": ProjectionRefs.DataRef(name="liquidity"),
                "amount0": ProjectionRefs.DataRef(name="amount0"),
                "amount1": ProjectionRefs.DataRef(name="amount1"),
            },
            fast_zero_words=(1, 2, 3),
            drop_if_all_zero_fields=("liquidity", "amount0", "amount1"),
        ),
    )

    # Burn
    add_event_spec(
        reg,
        EventSpec(
            topic0=BURN_T0,
            name="Burn",
            topic_fields=[
                TopicFieldSpec("owner", 1, "address"),
                TopicFieldSpec("tick_lower", 2, "int24"),
                TopicFieldSpec("tick_upper", 3, "int24"),
            ],
            data_fields=[
                DataFieldSpec("liquidity", 0, "uint128"),
                DataFieldSpec("amount0", 1, "uint256"),
                DataFieldSpec("amount1", 2, "uint256"),
            ],
            projection={
                "owner": ProjectionRefs.TopicRef(name="owner"),
                "tick_lower": ProjectionRefs.TopicRef(name="tick_lower"),
                "tick_upper": ProjectionRefs.TopicRef(name="tick_upper"),
                "liquidity": ProjectionRefs.DataRef(name="liquidity"),
                "amount0": ProjectionRefs.DataRef(name="amount0"),
                "amount1": ProjectionRefs.DataRef(name="amount1"),
            },
            fast_zero_words=(0, 1, 2),
            drop_if_all_zero_fields=("liquidity", "amount0", "amount1"),
        ),
    )

    # Collect
    add_event_spec(
        reg,
        EventSpec(
            topic0=COLLECT_T0,
            name="Collect",
            topic_fields=[
                TopicFieldSpec("owner", 1, "address"),
                TopicFieldSpec("tick_lower", 2, "int24"),
                TopicFieldSpec("tick_upper", 3, "int24"),
            ],
            data_fields=[
                DataFieldSpec("recipient", 0, "address"),
                DataFieldSpec("amount0", 1, "uint128"),
                DataFieldSpec("amount1", 2, "uint128"),
            ],
            projection={
                "owner": ProjectionRefs.TopicRef(name="owner"),
                "recipient": ProjectionRefs.DataRef(name="recipient"),
                "tick_lower": ProjectionRefs.TopicRef(name="tick_lower"),
                "tick_upper": ProjectionRefs.TopicRef(name="tick_upper"),
                "amount0": ProjectionRefs.DataRef(name="amount0"),
                "amount1": ProjectionRefs.DataRef(name="amount1"),
            },
            fast_zero_words=(1, 2),
            drop_if_all_zero_fields=("amount0", "amount1"),
        ),
    )

    # CollectFees
    add_event_spec(
        reg,
        EventSpec(
            topic0=COLLECTFEES_T0,
            name="CollectFees",
            topic_fields=[TopicFieldSpec("recipient", 1, "address")],
            data_fields=[
                DataFieldSpec("amount0", 0, "uint128"),
                DataFieldSpec("amount1", 1, "uint128"),
            ],
            projection={
                "recipient": ProjectionRefs.TopicRef(name="recipient"),
                "amount0": ProjectionRefs.DataRef(name="amount0"),
                "amount1": ProjectionRefs.DataRef(name="amount1"),
            },
            fast_zero_words=(0, 1),
            drop_if_all_zero_fields=("amount0", "amount1"),
        ),
    )

    return reg


# -------------------------
# Gauge registry (Deposit/Withdraw/ClaimRewards)
# -------------------------


def make_gauge_registry() -> EventRegistry:
    """Return registry for gauge events (deposit/withdraw/claim)."""
    reg: EventRegistry = {}

    # Deposit
    add_event_spec(
        reg,
        EventSpec(
            topic0=DEPOSIT_T0,
            name="Deposit",
            topic_fields=[
                TopicFieldSpec("user", 1, "address"),
                TopicFieldSpec("tokenId", 2, "uint256"),
                TopicFieldSpec("liquidityToStake", 3, "uint128"),
            ],
            data_fields=[],
            projection={
                "owner": ProjectionRefs.TopicRef(name="user"),
                "liquidity": ProjectionRefs.TopicRef(name="liquidityToStake"),
                "token_id": ProjectionRefs.TopicRef(name="tokenId"),
            },
            fast_zero_words=(),
            drop_if_all_zero_fields=(),
        ),
    )

    # Withdraw
    add_event_spec(
        reg,
        EventSpec(
            topic0=WITHDRAW_T0,
            name="Withdraw",
            topic_fields=[
                TopicFieldSpec("user", 1, "address"),
                TopicFieldSpec("tokenId", 2, "uint256"),
                TopicFieldSpec("liquidityToStake", 3, "uint128"),
            ],
            data_fields=[],
            projection={
                "owner": ProjectionRefs.TopicRef(name="user"),
                "liquidity": ProjectionRefs.TopicRef(name="liquidityToStake"),
                "token_id": ProjectionRefs.TopicRef(name="tokenId"),
            },
            fast_zero_words=(),
            drop_if_all_zero_fields=(),
        ),
    )

    # ClaimRewards
    add_event_spec(
        reg,
        EventSpec(
            topic0=CLAIM_REWARDS_T0,
            name="ClaimRewards",
            topic_fields=[TopicFieldSpec("from", 1, "address")],
            data_fields=[DataFieldSpec("amount", 0, "uint256")],
            projection={
                "owner": ProjectionRefs.TopicRef(name="from"),
                "amount": ProjectionRefs.DataRef(name="amount"),
            },
            fast_zero_words=(),
            drop_if_all_zero_fields=(),
        ),
    )

    return reg


# -------------------------
# VFAT / Sickle registry
# -------------------------


def make_vfat_registry() -> EventRegistry:
    """Return registry for VFAT/Sickle-related events."""

    reg: EventRegistry = {}

    add_event_spec(
        reg,
        EventSpec(
            topic0=DEPLOY_T0,
            name="Deploy",
            topic_fields=[
                TopicFieldSpec("admin", 1, "address"),
            ],
            data_fields=[
                DataFieldSpec("sickle", 0, "address"),
            ],
            projection={
                "admin": ProjectionRefs.TopicRef(name="admin"),
                "sickle": ProjectionRefs.DataRef(name="sickle"),
            },
            fast_zero_words=(),
            drop_if_all_zero_fields=(),
        ),
    )

    return reg


# -------------------------
# NFPM (NonFungiblePositionManager) registry
# -------------------------


def make_nfpm_registry() -> EventRegistry:
    """Return registry for NonFungiblePositionManager events."""
    reg: EventRegistry = {}

    # Collect(uint256 indexed tokenId, address recipient, uint256 amount0, uint256 amount1)
    add_event_spec(
        reg,
        EventSpec(
            topic0="0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01",
            name="Collect",
            topic_fields=[TopicFieldSpec("tokenId", 1, "uint256")],
            data_fields=[
                DataFieldSpec("recipient", 0, "address"),
                DataFieldSpec("amount0", 1, "uint256"),
                DataFieldSpec("amount1", 2, "uint256"),
            ],
            projection={
                "token_id": ProjectionRefs.TopicRef(name="tokenId"),
                "recipient": ProjectionRefs.DataRef(name="recipient"),
                "amount0": ProjectionRefs.DataRef(name="amount0"),
                "amount1": ProjectionRefs.DataRef(name="amount1"),
            },
            fast_zero_words=(1, 2),
            drop_if_all_zero_fields=("amount0", "amount1"),
        ),
    )

    # IncreaseLiquidity(uint256 indexed tokenId, uint128 liquidity, uint256 amount0, uint256 amount1)
    add_event_spec(
        reg,
        EventSpec(
            topic0="0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f",
            name="IncreaseLiquidity",
            topic_fields=[TopicFieldSpec("tokenId", 1, "uint256")],
            data_fields=[
                DataFieldSpec("liquidity", 0, "uint128"),
                DataFieldSpec("amount0", 1, "uint256"),
                DataFieldSpec("amount1", 2, "uint256"),
            ],
            projection={
                "token_id": ProjectionRefs.TopicRef(name="tokenId"),
                "liquidity": ProjectionRefs.DataRef(name="liquidity"),
                "amount0": ProjectionRefs.DataRef(name="amount0"),
                "amount1": ProjectionRefs.DataRef(name="amount1"),
            },
            fast_zero_words=(0, 1, 2),
            drop_if_all_zero_fields=("liquidity", "amount0", "amount1"),
        ),
    )

    # DecreaseLiquidity(uint256 indexed tokenId, uint128 liquidity, uint256 amount0, uint256 amount1)
    add_event_spec(
        reg,
        EventSpec(
            topic0="0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4",
            name="DecreaseLiquidity",
            topic_fields=[TopicFieldSpec("tokenId", 1, "uint256")],
            data_fields=[
                DataFieldSpec("liquidity", 0, "uint128"),
                DataFieldSpec("amount0", 1, "uint256"),
                DataFieldSpec("amount1", 2, "uint256"),
            ],
            projection={
                "token_id": ProjectionRefs.TopicRef(name="tokenId"),
                "liquidity": ProjectionRefs.DataRef(name="liquidity"),
                "amount0": ProjectionRefs.DataRef(name="amount0"),
                "amount1": ProjectionRefs.DataRef(name="amount1"),
            },
            fast_zero_words=(0, 1, 2),
            drop_if_all_zero_fields=("liquidity", "amount0", "amount1"),
        ),
    )

    # Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
    add_event_spec(
        reg,
        EventSpec(
            topic0="0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            name="Transfer",
            topic_fields=[
                TopicFieldSpec("from", 1, "address"),
                TopicFieldSpec("to", 2, "address"),
                TopicFieldSpec("tokenId", 3, "uint256"),
            ],
            data_fields=[],
            projection={
                "from": ProjectionRefs.TopicRef(name="from"),
                "to": ProjectionRefs.TopicRef(name="to"),
                "token_id": ProjectionRefs.TopicRef(name="tokenId"),
            },
            fast_zero_words=(),
            drop_if_all_zero_fields=(),
        ),
    )

    return reg
