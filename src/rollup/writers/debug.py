from __future__ import annotations

from pathlib import Path

import polars as pl

from rollup.writers import parquet


def validate(
    output_root: Path,
    *,
    sources: dict[str, pl.DataFrame | pl.LazyFrame],
    seeds: dict[str, pl.DataFrame | pl.LazyFrame],
    staging: dict[str, pl.DataFrame | pl.LazyFrame],
    intermediate: dict[str, pl.DataFrame | pl.LazyFrame],
    marts: dict[str, pl.DataFrame | pl.LazyFrame],
) -> None:
    if not isinstance(output_root, Path):
        raise TypeError("debug writer: output_root must be a pathlib.Path")
    for layer_name, frames in {
        "sources": sources,
        "seeds": seeds,
        "staging": staging,
        "intermediate": intermediate,
        "marts": marts,
    }.items():
        if not isinstance(frames, dict):
            raise TypeError(f"debug writer: {layer_name} must be a mapping")
        for name, frame in frames.items():
            if not isinstance(name, str) or not name:
                raise ValueError(
                    f"debug writer: {layer_name} frame names must be non-empty strings"
                )
            if not isinstance(frame, pl.DataFrame | pl.LazyFrame):
                raise TypeError(
                    f"debug writer: {layer_name} frame '{name}' must be a Polars DataFrame or LazyFrame"
                )


def write(
    output_root: Path,
    *,
    sources: dict[str, pl.DataFrame | pl.LazyFrame],
    seeds: dict[str, pl.DataFrame | pl.LazyFrame],
    staging: dict[str, pl.DataFrame | pl.LazyFrame],
    intermediate: dict[str, pl.DataFrame | pl.LazyFrame],
    marts: dict[str, pl.DataFrame | pl.LazyFrame],
) -> Path:
    validate(
        output_root,
        sources=sources,
        seeds=seeds,
        staging=staging,
        intermediate=intermediate,
        marts=marts,
    )
    debug_dir = output_root / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    for prefix, frames in {
        "src": sources,
        "seed": seeds,
        "stg": staging,
        "int": intermediate,
        "mts": marts,
    }.items():
        for name, frame in frames.items():
            parquet.write(frame, debug_dir / f"{prefix}_{name}.parquet")
    return debug_dir
