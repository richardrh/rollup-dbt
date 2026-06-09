from __future__ import annotations

from pathlib import Path

import polars as pl

from rollup.columns import Col
from rollup.intermediate.build_metric_long import METRIC_LONG_SCHEMA
from rollup.metric_names import loss_blended_fx_forecast_euws_override_metric
from rollup.marts.write_parquet import write_parquet


FANOUT_INPUT_SCHEMA = METRIC_LONG_SCHEMA


def write_fanouts(
    marts_dir: Path,
    frame: pl.DataFrame | pl.LazyFrame,
    target_currency: str = "GBP",
) -> tuple[Path, ...]:
    FANOUT_INPUT_SCHEMA.validate(frame)

    paths: list[Path] = []
    source = frame.lazy() if isinstance(frame, pl.DataFrame) else frame
    main = source.filter(pl.col(Col.metric) == loss_blended_fx_forecast_euws_override_metric(target_currency))
    keys = main.select(Col.base_model, Col.forecast_date).unique().sort(Col.base_model, Col.forecast_date).collect()
    if keys.is_empty():
        return ()
    for row in keys.iter_rows(named=True):
        subset = main.filter(
            (pl.col(Col.base_model) == row[Col.base_model])
            & (pl.col(Col.forecast_date) == row[Col.forecast_date])
        )
        vendor = "HiscoAIR" if row[Col.base_model] == "verisk" else "HiscoRMS"
        forecast = str(row[Col.forecast_date]).replace("-", "")
        path = marts_dir / f"{vendor}_{forecast}_main.parquet"
        write_parquet(subset, path)
        paths.append(path)
    return tuple(sorted(paths))
