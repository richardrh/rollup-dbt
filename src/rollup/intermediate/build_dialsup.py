from __future__ import annotations

import polars as pl

from rollup.columns import Col


def build_dialsup(metric_long: pl.LazyFrame) -> pl.LazyFrame:
    return metric_long.filter(
        (pl.col(Col.is_dialsup) == 1) & (pl.col(Col.metric) == "forecast")
    ).with_columns(pl.lit("dialsup_gbp_forecast").alias(Col.metric))
