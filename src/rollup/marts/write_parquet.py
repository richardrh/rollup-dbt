from __future__ import annotations

from pathlib import Path

import polars as pl


def write_parquet(frame: pl.DataFrame | pl.LazyFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(frame, pl.LazyFrame):
        frame.collect().write_parquet(path)
    else:
        frame.write_parquet(path)
