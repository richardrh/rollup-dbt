from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.intermediate.build_metric_long import METRIC_LONG_SCHEMA


DIALSUP_INPUT_SCHEMA = METRIC_LONG_SCHEMA
DIALSUP_SCHEMA = METRIC_LONG_SCHEMA


def build_dialsup(metric_long: pl.LazyFrame) -> pl.LazyFrame:
    DIALSUP_INPUT_SCHEMA.validate(metric_long)

    dialsup = metric_long.filter(
        (pl.col(Col.is_dialsup) == 1) & (pl.col(Col.metric) == "forecast")
    ).with_columns(pl.lit("dialsup_gbp_forecast").alias(Col.metric))
    return dialsup
