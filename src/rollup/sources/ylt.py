"""Discover vendor YLT parquet inputs as lazy Polars frames."""

from __future__ import annotations

from pathlib import Path

import polars as pl


def load(data_root: Path | str = "data") -> dict[str, pl.LazyFrame]:
    """Load top-level vendor YLT parquet paths without collecting rows."""
    data_root = Path(data_root)
    frames: dict[str, pl.LazyFrame] = {}
    for vendor in ("verisk", "risklink"):
        vendor_root = data_root / "ylt" / vendor
        vendor_paths = sorted(vendor_root.glob("*.parquet"))
        if not vendor_paths:
            raise FileNotFoundError(
                f"no {vendor} YLT parquet files found in {vendor_root}"
            )
        frames[vendor] = pl.scan_parquet(vendor_paths)
    return frames
