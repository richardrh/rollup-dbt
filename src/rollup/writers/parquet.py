from __future__ import annotations

import logging
import time
import tempfile
from pathlib import Path

import polars as pl

logger = logging.getLogger("rollup.pipeline")


def validate(frame: pl.DataFrame | pl.LazyFrame, output_path: Path) -> None:
    if not isinstance(frame, pl.DataFrame | pl.LazyFrame):
        raise TypeError("parquet writer: frame must be a Polars DataFrame or LazyFrame")
    if not isinstance(output_path, Path):
        raise TypeError("parquet writer: output_path must be a pathlib.Path")
    if output_path.suffix != ".parquet":
        raise ValueError("parquet writer: output_path must have a .parquet suffix")
    if isinstance(frame, pl.LazyFrame):
        try:
            frame.collect_schema()
        except Exception as exc:
            raise ValueError(
                "parquet writer: unable to resolve lazy frame schema"
            ) from exc


def write(frame: pl.DataFrame | pl.LazyFrame, output_path: Path) -> Path:
    validate(frame, output_path)
    started = time.perf_counter()
    lazy = isinstance(frame, pl.LazyFrame)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        suffix=".parquet",
        prefix=f".{output_path.stem}-",
        dir=output_path.parent,
        delete=False,
    ) as handle:
        staged_path = Path(handle.name)
    try:
        if isinstance(frame, pl.LazyFrame):
            frame.sink_parquet(staged_path)
            row_count = -1
        else:
            frame.write_parquet(staged_path)
            row_count = frame.height
        staged_path.replace(output_path)
    finally:
        staged_path.unlink(missing_ok=True)
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
    return output_path
