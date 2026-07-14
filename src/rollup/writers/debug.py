from __future__ import annotations
# mypy: ignore-errors

from pathlib import Path

import polars as pl

from rollup.writers.parquet import write_parquet_with_log


def write_debug_frame(
    debug_dir: Path,
    name: str,
    frame: pl.DataFrame | pl.LazyFrame,
) -> None:
    debug_dir.mkdir(parents=True, exist_ok=True)
    output_path = debug_dir / f"{name}.parquet"
    write_parquet_with_log(frame, output_path)


def write_debug_outputs(
    output_root: Path,
    *,
    seeds: dict[str, pl.DataFrame | pl.LazyFrame],
    staging: dict[str, pl.DataFrame | pl.LazyFrame],
    intermediate: dict[str, pl.DataFrame | pl.LazyFrame],
    marts: dict[str, pl.DataFrame | pl.LazyFrame],
) -> None:
    debug_dir = output_root / "debug"
    for prefix, frames in {
        "seed": seeds,
        "stg": staging,
        "int": intermediate,
        "mts": marts,
    }.items():
        for name, frame in frames.items():
            write_debug_frame(debug_dir, f"{prefix}_{name}", frame)
