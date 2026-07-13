from __future__ import annotations
# mypy: ignore-errors

import logging
import time
from pathlib import Path

import polars as pl


logger = logging.getLogger("rollup.pipeline")


def write_parquet_with_log(frame: pl.DataFrame | pl.LazyFrame, output_path: Path) -> None:
    started = time.perf_counter()
    lazy = isinstance(frame, pl.LazyFrame)
    if isinstance(frame, pl.LazyFrame):
        frame.sink_parquet(output_path, mkdir=True)
        row_count = -1
    else:
        frame.write_parquet(output_path)
        row_count = frame.height
    elapsed_seconds = time.perf_counter() - started
    logger.info(
        "wrote output=%s rows=%d elapsed=%.2fs",
        output_path,
        row_count,
        elapsed_seconds,
        extra={
            "event": "write_output",
            "path": output_path,
            "rows": row_count,
            "elapsed_seconds": elapsed_seconds,
            "lazy": lazy,
        },
    )
