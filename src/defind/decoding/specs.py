"""Event specification primitives and registry typing.

Defines lightweight dataclasses to describe how to decode events:
- `TopicFieldSpec` / `DataFieldSpec`: typed sources for indexed topics / data words
- `EventSpec`: one event rule (topic0, fields, dynamic projection, filters)
- `EventRegistry`: mapping from topic0 → EventSpec
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any


# ---- Dynamic projection mapping ----
# Keys: output column names (e.g., "amount0", "owner", "liquidity")
# Values: references to parsed fields or constant strings
#   - ProjectionRefs.TopicRef(name="<name>")  → take from parsed indexed topic fields
#   - ProjectionRefs.DataRef(name="<name>")   → take from parsed data words
#   - ProjectionRefs.Constant(value="value")  → output a constant string
class ProjectionRefs:
    @dataclass(kw_only=True)
    class TopicRef:
        name: str

    @dataclass(kw_only=True)
    class DataRef:
        name: str

    @dataclass(kw_only=True)
    class Constant:
        value: str


ProjectionRef = ProjectionRefs.TopicRef | ProjectionRefs.DataRef | ProjectionRefs.Constant | None

Projection = Mapping[str, ProjectionRef]


def resolve_projection_ref(
    ref: ProjectionRef,
    topic_vals: dict[str, Any],
    data_vals: dict[str, Any],
) -> Any:
    """Resolve a projection reference"""
    if ref is None:
        return None
    match ref:
        case ProjectionRefs.TopicRef():
            return topic_vals.get(ref.name)
        case ProjectionRefs.DataRef():
            return data_vals.get(ref.name)
        case ProjectionRefs.Constant():
            return ref.value
    raise RuntimeError("Unsupported ProjectionEntry type")


@dataclass(frozen=True)
class TopicFieldSpec:
    """Describe one indexed topic field (by 0-based topic index and ABI type)."""

    name: str
    index: int
    type: str  # e.g., "address", "uint256", "int24", "bytes32"


@dataclass(frozen=True)
class DataFieldSpec:
    """Describe one 32-byte ABI word in the data section (0-based word index)."""

    name: str
    word_index: int
    type: str  # e.g., "address", "uint256", "uint128"


@dataclass(frozen=True)
class EventSpec:
    """One event decoding rule + dynamic projection."""

    topic0: str
    name: str
    topic_fields: list[TopicFieldSpec]
    data_fields: list[DataFieldSpec]
    projection: Projection
    fast_zero_words: tuple[int, ...] = ()
    drop_if_all_zero_fields: tuple[str, ...] = ()

    def __post_init__(self):
        def _find_matches(ref_name: str, fields: Sequence[TopicFieldSpec | DataFieldSpec]):
            return [field for field in fields if field.name == ref_name]

        for name, projection_ref in self.projection.items():
            if not isinstance(projection_ref, ProjectionRef):
                raise ValueError(f"{name} projection is not a ProjectionRef instance")
            match projection_ref:
                case ProjectionRefs.TopicRef():
                    matches = _find_matches(projection_ref.name, self.topic_fields)
                    if len(matches) == 0:
                        raise ValueError(f"{name} projection refers to an non-existant topic field")
                case ProjectionRefs.DataRef():
                    matches = _find_matches(projection_ref.name, self.data_fields)
                    if len(matches) == 0:
                        raise ValueError(f"{name} projection refers to an non-existant data field")


# The full registry keyed by topic0 (lowercased 0x-hex).
EventRegistry = dict[str, EventSpec]


def get_event_specs_topic0s(event_specs: Iterable[EventSpec]):
    return [event_spec.topic0 for event_spec in event_specs]


def get_event_registry_topic0s(registry: EventRegistry):
    return get_event_specs_topic0s(registry.values())
