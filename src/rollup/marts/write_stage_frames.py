from __future__ import annotations

from pathlib import Path

import polars as pl

from rollup.config import RollupConfig
from rollup.marts.write_parquet import write_parquet


def write_stage_frames(
    output_root: Path,
    section: str,
    frames: dict[str, pl.DataFrame | pl.LazyFrame],
    config: RollupConfig,
) -> tuple[Path, ...]:
    if not config.outputs.write_stage_outputs:
        return ()
    base = output_root / config.outputs.stage_output_dir / section
    base.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, frame in frames.items():
        path = base / f"{name}.parquet"
        write_parquet(frame, path)
        paths.append(path)
    return tuple(paths)
