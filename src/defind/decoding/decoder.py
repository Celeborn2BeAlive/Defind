"""Generic event decoder using dynamic projections.

This module translates raw logs into `ParsedEvent` using an `EventRegistry`
defined by `EventSpec` + (topic|data) field specs. Projections are dynamic:
any key defined in the registry's `projection` mapping becomes a column in
the universal buffer.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from eth_utils import to_checksum_address  # type: ignore[attr-defined]

from defind.core.models import Meta
from defind.decoding.specs import EventRegistry, EventSpec, resolve_projection_ref
from defind.decoding.utils import parse_data_word, parse_topic_field, word_at

# ---------- parsed event ----------


@dataclass(slots=True)
class ParsedEvent:
    """Decoded event with open-ended `values` for dynamic projections."""

    name: str
    pool: str
    meta: Meta
    values: dict[str, Any]


# ---------- helper functions ----------


def _validate_and_get_spec(topics: Sequence[str], registry: EventRegistry) -> EventSpec | None:
    """Validate topics and retrieve event spec from registry.

    Returns None if topics are invalid or spec not found.
    """
    if not topics:
        return None
    topic0 = topics[0].lower()
    return registry.get(topic0)


def _should_skip_fast_zero(spec: EventSpec, data: bytes) -> bool:
    """Check if event should be skipped based on fast-zero word check.

    Returns True if all designated data words are zero.
    """
    if not spec.fast_zero_words:
        return False

    for wi in spec.fast_zero_words:
        w = word_at(data, wi)
        if any(w):
            return False
    return True  # all designated words were zero


def _should_skip_all_zero_fields(spec: EventSpec, data_vals: dict[str, Any]) -> bool:
    """Check if event should be skipped based on all-zero fields check.

    Returns True if all specified fields are zero.
    """
    if not spec.drop_if_all_zero_fields:
        return False

    return all(int(data_vals.get(name, 0) or 0) == 0 for name in spec.drop_if_all_zero_fields)


# ---------- main generic decoder (dynamic) ----------


def decode_event(
    *,
    topics: Sequence[str],
    data: bytes,
    meta: Meta,
    registry: EventRegistry,
) -> ParsedEvent | None:
    """Decode raw log (topics + data) into a `ParsedEvent` or return None if filtered.

    Filtering rules (optional per `EventSpec`):
    - `fast_zero_words`: if all designated data words are zero → drop early.
    - `drop_if_all_zero_fields`: after typed parse, if all listed fields are zero → drop.
    """
    # Validate and get spec
    spec = _validate_and_get_spec(topics, registry)
    if spec is None:
        return None

    # Early fast-zero check on specified data words
    if _should_skip_fast_zero(spec, data):
        return None

    # Parse topic fields
    topic_vals: dict[str, Any] = {}
    for tf in spec.topic_fields:
        if tf.index >= len(topics):
            return None
        topic_vals[tf.name] = parse_topic_field(topics[tf.index], tf)

    # Parse data words (ensure data size)
    if spec.data_fields:
        need_words = max(df.word_index for df in spec.data_fields) + 1
        if len(data) < 32 * need_words:
            return None

    data_vals: dict[str, Any] = {}
    for df in spec.data_fields:
        data_vals[df.name] = parse_data_word(word_at(data, df.word_index), df.type)

    # Drop if all specified fields are zero (post-parse)
    if _should_skip_all_zero_fields(spec, data_vals):
        return None

    # Dynamically resolve ALL projection keys
    resolved: dict[str, Any] = {}
    for out_key, ref in spec.projection.items():
        v = resolve_projection_ref(ref, topic_vals, data_vals)
        if isinstance(v, int):
            v = str(v)  # Arrow safety for big ints
        resolved[out_key] = v

    return ParsedEvent(
        name=spec.name,
        pool=to_checksum_address(meta.pool),
        meta=meta,
        values=resolved,  # <- open-ended dict
    )
