from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from defind.core.config import OrchestratorConfig
from defind.decoding.specs import EventRegistry
from defind.orchestration.orchestrator import SetupDirectoriesResult, fetch_decode


@pytest.mark.asyncio
async def test_fetch_decode_empty_seeds(mock_rpc: Any) -> None:
    # Mock internal components to avoid file I/O and complex setup
    with (
        patch(
            "defind.orchestration.orchestrator._setup_directories",
            return_value=SetupDirectoriesResult(
                key_dir=Path("/tmp/key"),
                manifests_dir=Path("/tmp/manifests"),
            ),
        ),
        patch("defind.orchestration.orchestrator.LiveManifest") as MockManifest,
        patch("defind.orchestration.orchestrator.load_done_coverage", return_value=[]),
        patch("defind.orchestration.orchestrator._build_work_seeds", return_value=[]),
        patch("defind.orchestration.orchestrator.RPC", return_value=mock_rpc),
    ):
        MockManifest.return_value.append = AsyncMock()
        registry = EventRegistry()
        config = OrchestratorConfig(
            rpc_url="http://localhost:8545",
            address="0x123",
            topic0s=["0xabc"],
            start_block=0,
            end_block=100,
        )
        fetch_decode_output = await fetch_decode(
            config=config,
            registry=registry,
        )

        assert fetch_decode_output.stats.processed_ok == 0
        assert fetch_decode_output.stats.total_logs == 0


@pytest.mark.asyncio
async def test_fetch_decode_with_work(mock_rpc: Any) -> None:
    # Mock logs return
    mock_log = MagicMock()
    mock_log.topics = ["0xabc"]
    mock_log.data_hex = "0x00"
    mock_log.block_number = 10
    mock_log.block_timestamp = 1000
    mock_log.tx_hash = "0xtx"
    mock_log.log_index = 0
    mock_log.address = "0x123"

    mock_rpc.get_logs.return_value = [mock_log]

    with (
        patch(
            "defind.orchestration.orchestrator._setup_directories",
            return_value=SetupDirectoriesResult(
                key_dir=Path("/tmp/key"),
                manifests_dir=Path("/tmp/manifests"),
            ),
        ),
        patch("defind.orchestration.orchestrator.LiveManifest") as MockManifest,
        patch("defind.orchestration.orchestrator.load_done_coverage", return_value=[]),
        patch("defind.orchestration.orchestrator._build_work_seeds", return_value=[(0, 100)]),
        patch("defind.orchestration.orchestrator.RPC", return_value=mock_rpc),
        patch("defind.orchestration.orchestrator.ShardWriter"),
        patch("defind.orchestration.orchestrator.decode_event") as mock_decode,
    ):
        MockManifest.return_value.append = AsyncMock()
        # Mock decode result
        mock_decode.return_value = MagicMock(name="TestEvent", values={}, pool="0x123")

        registry = EventRegistry()
        config = OrchestratorConfig(
            rpc_url="http://localhost:8545",
            address="0x123",
            topic0s=["0xabc"],
            start_block=0,
            end_block=100,
        )
        fetch_decode_output = await fetch_decode(
            config=config,
            registry=registry,
        )

        assert fetch_decode_output.stats.executed_subranges == 1
        assert fetch_decode_output.stats.total_logs == 1
        assert fetch_decode_output.stats.processed_ok == 1
