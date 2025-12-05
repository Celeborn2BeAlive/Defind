import json
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any, Literal

from eth_utils.abi import event_signature_to_log_topic
from pydantic import BaseModel

from defind.decoding.registry import add_event_spec
from defind.decoding.specs import DataFieldSpec, EventRegistry, EventSpec, ProjectionRefs, TopicFieldSpec


class AbiInput(BaseModel):
    indexed: bool
    internalType: str
    name: str
    type: str


class AbiEvent(BaseModel):
    anonymous: bool
    inputs: Sequence[AbiInput]
    name: str
    type: Literal["event"]


def get_event_signature(event: AbiEvent):
    return f"{event.name}({','.join(event_input.type for event_input in event.inputs)})"


def get_event_topic0(event: AbiEvent):
    return "0x" + event_signature_to_log_topic(get_event_signature(event)).hex()


def get_event_topic_field_specs(event: AbiEvent):
    return [
        TopicFieldSpec(event_input.name, event_input_idx + 1, event_input.type)
        for event_input_idx, event_input in enumerate(
            [event_input for event_input in event.inputs if event_input.indexed]
        )
    ]


def get_event_data_field_specs(event: AbiEvent):
    return [
        DataFieldSpec(event_input.name, event_input_idx, event_input.type)
        for event_input_idx, event_input in enumerate(
            [event_input for event_input in event.inputs if not event_input.indexed]
        )
    ]


def get_event_projection_ref(event_input: AbiInput):
    if event_input.indexed:
        return ProjectionRefs.TopicRef(name=event_input.name)
    else:
        return ProjectionRefs.DataRef(name=event_input.name)


def get_event_spec(event: AbiEvent):
    return EventSpec(
        topic0=get_event_topic0(event),
        name=event.name,
        topic_fields=get_event_topic_field_specs(event),
        data_fields=get_event_data_field_specs(event),
        projection={event_input.name: get_event_projection_ref(event_input) for event_input in event.inputs},
        # fast_zero_words=(1, 2, 3),
        # drop_if_all_zero_fields=("liquidity", "amount0", "amount1"),
    )


AbiJson = Iterable[dict[str, Any]]
AbiSpec = AbiJson | Path


def _load_abi(abi: AbiSpec) -> AbiJson:
    if isinstance(abi, Path):
        return json.loads(abi.read_text())
    return abi


def get_events_from_abi(abi: AbiSpec) -> dict[str, AbiEvent]:
    abi = _load_abi(abi)
    return {entry["name"]: AbiEvent.model_validate(entry) for entry in abi if entry["type"] == "event"}


def make_event_registry_from_events(events: Iterable[AbiEvent]) -> EventRegistry:
    reg: EventRegistry = {}

    for event in events:
        add_event_spec(
            reg,
            get_event_spec(event),
        )

    return reg


def make_event_registry_from_abi(abi: AbiSpec) -> EventRegistry:
    return make_event_registry_from_events(get_events_from_abi(abi).values())
