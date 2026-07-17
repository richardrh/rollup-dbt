"""Discover seed files as lazy Polars frames."""

from __future__ import annotations

from pathlib import Path

import polars as pl


def load(data_root: Path | str = "data") -> dict[str, pl.LazyFrame]:
    """Recursively discover CSV and parquet seeds without collecting rows."""
    seed_root = Path(data_root) / "seeds"
    if not seed_root.exists():
        return {}

    seed_paths = sorted([*seed_root.rglob("*.csv"), *seed_root.rglob("*.parquet")])
    frames: dict[str, pl.LazyFrame] = {}
    previous_paths: dict[str, Path] = {}
    for seed_path in seed_paths:
        seed_name = seed_path.stem
        if seed_name in frames:
            previous_path = previous_paths[seed_name]
            raise ValueError(
                f"duplicate seed stem {seed_name!r}: {previous_path} and {seed_path}"
            )
        previous_paths[seed_name] = seed_path
        frames[seed_name] = (
            pl.scan_parquet(seed_path)
            if seed_path.suffix == ".parquet"
            else pl.scan_csv(seed_path)
        )
    return frames
