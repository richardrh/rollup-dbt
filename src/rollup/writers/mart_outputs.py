from __future__ import annotations
# mypy: ignore-errors

import logging
import tempfile
import time
from pathlib import Path

import polars as pl

from rollup.writers.wide_outputs import _write_combined_outputs
from rollup.writers.fanout_partitions import _write_fanout_partitions
from rollup.writers.parquet import write_parquet_with_log


logger = logging.getLogger(__name__)


def write_mart_outputs(output_root: Path, mart_frames: dict[str, pl.LazyFrame]) -> None:
    output_dir = output_root / "marts"
    output_dir.mkdir(parents=True, exist_ok=True)
    for stale_mart in output_dir.glob("*.parquet"):
        stale_mart.unlink()

    fanouts: dict[str, pl.LazyFrame] = {}
    for name, frame in mart_frames.items():
        if not name.endswith("fanout"):
            continue
        fanouts[name] = frame

    ylt_long = mart_frames.get("ylt_long")
    ylt_dialsup = mart_frames.get("ylt_dialsup")
    if ylt_long is not None and ylt_dialsup is not None:
        _write_combined_outputs(output_root, ylt_long, ylt_dialsup)

    event_validation = mart_frames.get("event_validation")
    if event_validation is not None:
        write_parquet_with_log(event_validation, output_root / "mts_event_validation.parquet")

    for name, frame in fanouts.items():
        started = time.perf_counter()
        with tempfile.TemporaryDirectory(prefix=f"rollup-{name}-") as temp_dir:
            materialized_path = Path(temp_dir) / f"{name}.parquet"
            logger.info(
                "materializing fanout once fanout=%s output=%s",
                name,
                materialized_path,
                extra={"event": "fanout_materialize_start", "fanout": name, "path": materialized_path},
            )
            frame.sink_parquet(materialized_path)
            elapsed_seconds = time.perf_counter() - started
            logger.info(
                "materialized fanout fanout=%s elapsed=%.2fs",
                name,
                elapsed_seconds,
                extra={"event": "fanout_materialize_done", "fanout": name, "elapsed_seconds": elapsed_seconds},
            )
            _write_fanout_partitions(name, materialized_path, output_dir, started)
