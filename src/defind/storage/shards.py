from __future__ import annotations

import glob
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from defind.core.models import Column


class ShardsDir:
    def __init__(
        self,
        *,
        out_root: Path,
        addr_slug: str,
        topics_fp: str,
    ):
        self.key_name = f"{addr_slug}__topics-{topics_fp}__universal"
        self.key_dir = out_root / self.key_name
        self.shards_dir = self.key_dir / "shards"
        self.shards_dir.mkdir(exist_ok=True, parents=True)

    def shards_files_pattern(self) -> str:
        return (self.shards_dir / "shard_*.parquet").as_posix()

    def list_shards(self) -> list[str]:
        return sorted(glob.glob(self.shards_files_pattern()))

    def shard_path(self, idx: int) -> Path:
        return self.shards_dir / f"shard_{idx:05d}.parquet"


class ShardWriter:
    """
    Shard writer using dynamic columns:
    any projected key becomes a new column on the fly.

    New behavior:
    - If the last existing shard is not full, we *append to it* (by rewriting) until it
      reaches `rows_per_shard`, even across brand-new runs. Only then do we advance to the
      next shard index.
    """

    def __init__(
        self,
        shards_dir: ShardsDir,
        *,
        rows_per_shard: int = 250_000,
        codec: str = "zstd",
        write_final_partial: bool = True,
    ) -> None:
        self.rows_per_shard = rows_per_shard
        self.codec = codec
        self.write_final_partial = write_final_partial

        self.shards_dir = shards_dir

        self.buf = Column.empty()

        # Partial-open shard state (if last shard < rows_per_shard)
        self._open_partial_idx: int | None = None
        self._open_partial_tbl: pa.Table | None = None
        self._open_partial_remaining: int = 0

        # Initialize writer state from existing shards
        self.shard_idx = self._init_from_existing()

    # ---------- init & helpers ----------

    def _init_from_existing(self) -> int:
        """Load last shard (if any). If it's partial, keep it open for topping up."""
        existing = self.shards_dir.list_shards()
        if not existing:
            return 0

        last_path = existing[-1]
        last_idx = int(os.path.basename(last_path).split("_")[1].split(".")[0])

        # Read only the metadata (cheap) to get row count.
        pf = pq.ParquetFile(last_path)
        last_rows = pf.metadata.num_rows

        if last_rows < self.rows_per_shard and last_rows > 0:
            # Keep partial shard "open" for efficient appending:
            # Load existing data into memory so we can rewrite atomically
            # when adding new rows, avoiding expensive file concatenation
            self._open_partial_idx = last_idx
            self._open_partial_tbl = pq.read_table(last_path)
            self._open_partial_remaining = self.rows_per_shard - last_rows
            return last_idx  # Don't advance index until shard is full
        else:
            # Last shard is full or empty, start with next index
            return last_idx + 1

    def _atomic_write(self, out_path: Path, table: pa.Table) -> Path | None:
        """Write Parquet atomically (tmp + replace)."""
        if len(table) == 0:
            return None
        tmp = out_path.with_suffix(".tmp")
        pq.write_table(table, tmp, compression=self.codec)
        os.replace(tmp, out_path)
        print(f"💾 wrote → {out_path}  (rows={len(table)}, cols={len(table.schema)})")
        return out_path

    def _shard_path(self, idx: int) -> Path:
        return self.shards_dir.shard_path(idx)

    @property
    def _has_open_partial(self) -> bool:
        """Check if there's an open partial shard ready to be topped up."""
        return self._open_partial_idx is not None and self._open_partial_remaining > 0

    def _top_up_partial_shard(self, rows_to_take: int) -> Path | None:
        """Top up the currently open partial shard with rows from buffer.

        Args:
            rows_to_take: Maximum number of rows to take from buffer

        Returns:
            Path of the written shard file, or None if nothing written
        """
        if not self._has_open_partial or rows_to_take == 0:
            return None

        take_n = min(self._open_partial_remaining, rows_to_take)
        slice_cols = self.buf.take_first(take_n)
        new_tbl = slice_cols.to_arrow_table()

        assert self._open_partial_tbl is not None
        assert self._open_partial_idx is not None
        updated_tbl = pa.concat_tables([self._open_partial_tbl, new_tbl], promote=True)

        out_path = self._atomic_write(self._shard_path(self._open_partial_idx), updated_tbl)

        self._open_partial_remaining -= take_n
        self._open_partial_tbl = updated_tbl if self._open_partial_remaining > 0 else None

        # If shard is now full, advance index and clear partial state
        if self._open_partial_remaining == 0:
            self.shard_idx = self._open_partial_idx or 0
            self._open_partial_idx = None
            self._open_partial_tbl = None

        return out_path

    # ---------- core API ----------

    def add(self, cols: Column) -> list[Path]:
        """Merge `cols` into the aggregator; write shards as they become full.

        Returns a list of shard paths that were written/rewritten.
        """
        written: list[Path] = []

        appended = self.buf.extend(cols)
        if appended == 0:
            return written

        # 1) If we have a partial last shard open, top it up first
        if self._has_open_partial and self.buf.size() > 0:
            out_path = self._top_up_partial_shard(self.buf.size())
            if out_path:
                written.append(out_path)

        # 2) Write any full shards from the buffer into *new* shard files
        while self.buf.size() >= self.rows_per_shard:
            slice_cols = self.buf.take_first(self.rows_per_shard)
            tbl = slice_cols.to_arrow_table()
            out_path = self._atomic_write(self._shard_path(self.shard_idx), tbl)
            if out_path:
                written.append(out_path)
                self.shard_idx += 1

        # 3) Leave any remaining (< rows_per_shard) rows in memory buffer
        return written

    def close(self) -> Path | None:
        """Flush remaining rows.

        Rules:
        - If a partial shard is currently open, we *can* add remaining rows to it,
          regardless of `write_final_partial`, because this doesn't create a new shard.
        - If no partial shard is open:
            * If `write_final_partial` is False and in-memory rows < rows_per_shard, drop them.
            * Else, write them as a final (possibly short) shard.
        Returns the last shard path written (if any).
        """
        size = self.buf.size()
        if size == 0:
            return None

        # If we still have a partial shard open, try topping it up one last time.
        last_path: Path | None = None
        if self._has_open_partial and size > 0:
            last_path = self._top_up_partial_shard(size)

        # Now decide whether to write a *new* final partial shard
        remaining = self.buf.size()
        if remaining == 0:
            return last_path

        if not self.write_final_partial and remaining < self.rows_per_shard:
            # Drop incomplete rows when write_final_partial=False
            self.buf = Column.empty()
            return last_path

        slice_cols = self.buf.take_first(remaining)
        tbl = slice_cols.to_arrow_table()
        out_path = self._atomic_write(self._shard_path(self.shard_idx), tbl)
        if out_path:
            self.shard_idx += 1
            return out_path
        return last_path
