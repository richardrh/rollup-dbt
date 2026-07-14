from __future__ import annotations
# mypy: ignore-errors

from pathlib import Path

import polars as pl


def load_ylt_frames(data_root: Path | str = "data") -> dict[str, pl.LazyFrame]:
    data_root = Path(data_root)
    frames: dict[str, pl.LazyFrame] = {}
    for vendor in ("verisk", "risklink"):
        folder = data_root / "ylt" / vendor
        paths = sorted(folder.glob("*.parquet"))
        if not paths:
            raise FileNotFoundError(f"no {vendor} YLT parquet files found in {folder}")
        frames[vendor] = pl.scan_parquet(paths)
    return frames
