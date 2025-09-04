import json, asyncio, time
import click
from rich.console import Console
from rich.progress import (
    Progress, BarColumn, TextColumn, TimeElapsedColumn,
    TimeRemainingColumn, MofNCompleteColumn, SpinnerColumn
)
from rich.panel import Panel
from .fetcher import load_manifest, append_manifest, ChunkRec

console = Console()

@click.group()
def cli():
    """DeFind — fast, Python-native DeFi log fetcher."""
@cli.command("fetch-logs")
@click.option("--rpc", required=True, help="RPC endpoint URL")
@click.option("--contract", required=True, help="Emitter contract address")
@click.option("--event", "events", multiple=True, help="Event topic0; repeat to OR")
@click.option("--from-block", type=int, required=True)
@click.option("--to-block", type=int, required=True)
@click.option("--step", type=int, default=1_000, show_default=True, help="Blocks per request")
@click.option("--concurrency", type=int, default=16, show_default=True, help="Max parallel requests")
@click.option("--jsonl-out", type=str, default="", help="Optional path to write raw logs (NDJSON)")
@click.option("--manifest", "manifest_path", type=str, default="", help="JSONL manifest path for resume/skip")
@click.option("--rerun-failed/--no-rerun-failed", default=False, show_default=True,
              help="If set, re-run chunks previously marked failed in the manifest")
def fetch_logs_cmd(rpc, contract, events, from_block, to_block, step, concurrency, jsonl_out,
                   manifest_path, rerun_failed):
    """Fetch logs for a contract across a block range and multiple topic0s with a live dashboard."""
    if not events:
        raise click.UsageError("Pass at least one --event (topic0)")

    import httpx
    from .utils import to_hex_block, normalize_topic0_list

    async def fetch_chunk(session: httpx.AsyncClient, from_blk: int, to_blk: int, topic0s: list[str]):
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getLogs",
            "params": [{
                "address": contract,
                "fromBlock": to_hex_block(from_blk),
                "toBlock": to_hex_block(to_blk),
                "topics": [normalize_topic0_list(topic0s)]
            }]
        }
        r = await session.post(rpc, json=payload)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            err = data["error"]
            msg = err.get("message") if isinstance(err, dict) else str(err)
            raise RuntimeError(f"RPC error: {msg}")
        return data.get("result", [])

    async def run():
        import pandas as pd
        import httpx

        # Build all chunk ranges
        blocks = list(range(from_block, to_block + 1, step))
        ranges = [(b, min(to_block, b + step - 1)) for b in blocks]

        # Manifest-aware skipping
        manifest = load_manifest(manifest_path) if manifest_path else {}
        def key(fb, tb): return (fb, tb)

        to_run = []
        skipped = 0
        for fb, tb in ranges:
            rec = manifest.get(key(fb, tb))
            if rec:
                if rec.status == "done":
                    skipped += 1
                    continue
                if rec.status == "failed" and not rerun_failed:
                    skipped += 1
                    continue
            to_run.append((fb, tb))

        total_chunks = len(to_run)
        processed_ok = 0
        processed_failed = 0
        retried = 0
        total_logs = 0
        t0 = time.time()
        manifest_lock = asyncio.Lock()

        # Minimal progress bar
        progress = Progress( SpinnerColumn(),
                            TextColumn("[bold]collecting data[/]"),
                            BarColumn(),
                            MofNCompleteColumn(),
                            TextColumn("•"),
                            TimeElapsedColumn(),
                            TextColumn("→"),
                            TimeRemainingColumn(),
                            TextColumn(" • {task.description}"),
                            transient=False,
                            expand=True,
                            )

        with progress:
            task = progress.add_task(
                description=f"{from_block:,}-{to_block:,}",
                total=total_chunks
            )
            sem = asyncio.Semaphore(concurrency)

            async def worker(from_blk: int, to_blk: int):
                nonlocal processed_ok, processed_failed, retried, total_logs
                tries = 0
                logs = []
                status = "failed"
                err = None
                while True:
                    tries += 1
                    try:
                        async with sem:
                            logs = await fetch_chunk(session, from_blk, to_blk, list(events))
                        total_logs += len(logs)
                        processed_ok += 1
                        status = "done"
                        break
                    except Exception as e:
                        err = f"{type(e).__name__}: {e}"
                        if tries >= 3:
                            processed_failed += 1
                            logs = []
                            status = "failed"
                            break
                        retried += 1
                        await asyncio.sleep(0.8 * tries)

                # Write parquet file per chunk if logs are present
                if logs:
                    df = pd.DataFrame(logs)
                    fname = f"chunk_{from_blk}_{to_blk}.parquet"
                    df.to_parquet(fname, engine="pyarrow", index=False)

                # Update manifest
                if manifest_path:
                    rec = ChunkRec(
                        from_block=from_blk,
                        to_block=to_blk,
                        status=status,
                        attempts=tries,
                        error=err,
                        logs=len(logs),
                        updated_at=time.time(),
                    )
                    await append_manifest(manifest_path, rec, manifest_lock)

                progress.advance(task, 1)
                return logs

            async with httpx.AsyncClient(timeout=20) as session:
                await asyncio.gather(*(worker(fb, tb) for (fb, tb) in to_run))

        elapsed = time.time() - t0
        console.print(f"[bold]done[/]: {total_logs} logs • {elapsed:.2f}s")
        console.print(
            f"[bold]summary[/]: "
            f"[green]processed_ok[/]={processed_ok}  "
            f"[red]processed_failed[/]={processed_failed}  "
            f"[yellow]skipped[/]={skipped}  "
            f"(chunks={len(ranges)}, planned={total_chunks}+skipped)"
        )


    try:
        asyncio.run(run())
    except RuntimeError as e:
        raise click.ClickException(str(e))
