import pytest

from defind.core.models import Meta
from defind.decoding.decoder import decode_event
from defind.decoding.specs import DataFieldSpec, EventRegistry, EventSpec, ProjectionRefs, TopicFieldSpec


@pytest.fixture
def sample_registry() -> EventRegistry:
    spec = EventSpec(
        topic0="0x123",
        name="TestEvent",
        topic_fields=[TopicFieldSpec("user", 1, "address")],
        data_fields=[DataFieldSpec("amount", 0, "uint256")],
        projection={
            "user": ProjectionRefs.TopicRef(name="user"),
            "amount": ProjectionRefs.DataRef(name="amount"),
        },
    )
    registry = EventRegistry()
    registry[spec.topic0] = spec
    return registry


def test_decode_event_success(sample_registry: EventRegistry) -> None:
    meta = Meta(1, 1000, "0xtx", 0, "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045")
    topics = ["0x123", "0x" + "0" * 24 + "1234567890123456789012345678901234567890"]
    data = bytes.fromhex("0000000000000000000000000000000000000000000000000000000000000064")  # 100

    parsed = decode_event(topics=topics, data=data, meta=meta, registry=sample_registry)

    assert parsed is not None
    assert parsed.name == "TestEvent"
    assert parsed.values["user"] == "0x1234567890123456789012345678901234567890"
    assert parsed.values["amount"] == "100"


def test_decode_event_unknown_topic(sample_registry: EventRegistry) -> None:
    meta = Meta(1, 1000, "0xtx", 0, "0xpool")
    topics = ["0x999"]  # Unknown topic
    data = b""

    parsed = decode_event(topics=topics, data=data, meta=meta, registry=sample_registry)

    assert parsed is None
