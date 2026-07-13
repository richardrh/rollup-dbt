from __future__ import annotations

from pathlib import Path

import polars as pl


def load_seed_frames(data_root: Path | str = "data") -> dict[str, pl.LazyFrame]:
    seeds_root = Path(data_root) / "seeds"
    if not seeds_root.exists():
        return {}

    paths = sorted([*seeds_root.rglob("*.csv"), *seeds_root.rglob("*.parquet")])
    frames: dict[str, pl.LazyFrame] = {}
    source_paths: dict[str, Path] = {}
    for path in paths:
        stem = path.stem
        if stem in frames:
            raise ValueError(f"duplicate seed stem {stem!r}: {source_paths[stem]} and {path}")
        source_paths[stem] = path
        frames[stem] = pl.scan_parquet(path) if path.suffix == ".parquet" else pl.scan_csv(path)
    return frames
