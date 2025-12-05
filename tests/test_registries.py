from pathlib import Path

from defind.abi_events import get_event_topic0, get_events_from_abi, make_event_registry_from_abi
from defind.decoding.registries import make_clpool_registry, make_gauge_registry, make_nfpm_registry, make_vfat_registry


def test_make_clpool_registry():
    make_clpool_registry()


def test_make_gauge_registry():
    make_gauge_registry()


def test_make_vfat_registry():
    make_vfat_registry()


def test_make_nfpm_registry():
    make_nfpm_registry()


def test_make_event_registry_from_abi():
    abi = Path(__file__).parent / "abi" / "aerodrome_clpool_abi.json"
    assert abi.is_file()
    registry = make_event_registry_from_abi(abi)
    events = get_events_from_abi(abi)
    assert len(registry) == 9  # ABI defines 9 events
    assert len(registry) == len(events)
    assert set(registry.keys()) == set([get_event_topic0(event) for event in events.values()])
