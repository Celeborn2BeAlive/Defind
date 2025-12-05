import asyncio
from pathlib import Path

import duckdb
import pandas as pd

from defind.abi_events import make_event_registry_from_abi
from defind.core.config import OrchestratorConfig
from defind.decoding.specs import get_event_registry_topic0s
from defind.orchestration.orchestrator import fetch_decode

EXAMPLES_ROOT = Path(__file__).parent
assert EXAMPLES_ROOT.name == "examples"
REPO_ROOT = EXAMPLES_ROOT.parent
assert (REPO_ROOT / ".git").is_dir()
OUT_ROOT = REPO_ROOT / "data_examples"

ABI = EXAMPLES_ROOT / "abi" / "aerodrome_clpool_abi.json"
assert ABI.is_file()

registry = make_event_registry_from_abi(ABI)
config = OrchestratorConfig(
    rpc_url="https://base-rpc.publicnode.com",
    address="0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59",  # Uniswap USDC/ETH v3 0.05%
    topic0s=get_event_registry_topic0s(registry),
    start_block=13899892,  # Creation block of the pool
    end_block="latest",
    out_root=OUT_ROOT,
    step=20_000,
)


async def fetch_data():
    result = await fetch_decode(
        config=config,
        registry=registry,
    )
    return result


async def main():
    result = await fetch_data()

    df = pd.read_parquet(
        result.shards_dir.shard_path(0),
    )  # Load first shard and print number of rows, columns and second record
    print(len(df))
    print(df.columns)
    print(df.to_dict("records")[1])

    # Use duckdb to read all shards as a SQL database and display Swaps
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='4GB'")

    q = f"""
    SELECT *
    FROM read_parquet(
    '{result.shards_dir.shards_files_pattern()}'
    )
    WHERE event='Swap'
    ORDER BY block_timestamp, log_index
    """
    df = con.execute(q).df()
    print(len(df))
    print(df["amount0"].head())
    print(df[["amount0", "amount1", "sqrtPriceX96"]])
    print(df["log_index"].head())


asyncio.run(main())
