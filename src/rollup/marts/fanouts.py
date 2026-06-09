from __future__ import annotations

from pathlib import Path

import polars as pl

from rollup.columns import Col
from rollup.intermediate.build_metric_long import METRIC_LONG_SCHEMA


FANOUT_INPUT_SCHEMA = METRIC_LONG_SCHEMA


def write_fanouts(marts_dir: Path, frame: pl.DataFrame) -> tuple[Path, ...]:
    actual = frame.schema
    missing = [str(name) for name in FANOUT_INPUT_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"write_fanouts missing columns: {missing}")

    paths: list[Path] = []
    if frame.is_empty():
        return ()
    for row in frame.select(Col.base_model, Col.forecast_date).unique().iter_rows(named=True):
        subset = frame.filter(
            (pl.col(Col.base_model) == row[Col.base_model])
            & (pl.col(Col.forecast_date) == row[Col.forecast_date])
            & (pl.col(Col.metric) == "euws_override")
        )
        vendor = "HiscoAIR" if row[Col.base_model] == "verisk" else "HiscoRMS"
        forecast = str(row[Col.forecast_date]).replace("-", "")
        path = marts_dir / f"{vendor}_{forecast}_main.parquet"
        subset.write_parquet(path)
        paths.append(path)
    return tuple(sorted(paths))
