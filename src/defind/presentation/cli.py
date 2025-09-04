import asyncio, hashlib, os
import typer
from ..adapters.rpc_httpx import HttpxRPC
from ..adapters.parquet_sink import ParquetEventSink
from ..adapters.manifest_jsonl import JSONLManifest
from ..application.use_cases import fetch_decode_persist
from ..domain.value_types import Address, Topic0

app = typer.Typer()

def _fingerprint(topic0s: list[str]) -> str:
    base = ",".join(sorted([t.lower() for t in topic0s]))
    import hashlib as h; return h.sha1(base.encode()).hexdigest()[:10]

@app.command()
def fetch(
    rpc_url: str,
    address: str,
    topic0: list[str],
    start_block: int,
    end_block: int,
    step: int = 10_000,
    concurrency: int = 8,
    out_dir: str = "logs_parquet",
):
    async def main():
        addr_slug = address.lower()
        topics_fp = _fingerprint(topic0)
        key_dir = os.path.join(out_dir, f"{addr_slug}__topics-{topics_fp}")
        os.makedirs(key_dir, exist_ok=True)
        manifest_path = os.path.join(key_dir, "manifests", f"run.jsonl")

        rpc = HttpxRPC(rpc_url)
        sink = ParquetEventSink(key_dir, addr_slug, topics_fp)
        manifest = JSONLManifest(manifest_path)

        res = await fetch_decode_persist(
            rpc=rpc, sink=sink, manifest=manifest,
            address=Address(addr_slug), topic0s=[Topic0(t) for t in topic0],
            start_block=start_block, end_block=end_block,
            step=step, concurrency=concurrency,
        )
        typer.echo(res)

    asyncio.run(main())

if __name__ == "__main__":
    app()
