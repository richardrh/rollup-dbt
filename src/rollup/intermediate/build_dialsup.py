from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.intermediate.build_metric_long import METRIC_LONG_SCHEMA
from rollup.schemas import require_columns


DIALSUP_INPUT_SCHEMA = METRIC_LONG_SCHEMA
DIALSUP_SCHEMA = METRIC_LONG_SCHEMA


def build_dialsup(metric_long: pl.LazyFrame) -> pl.LazyFrame:
    require_columns(metric_long, DIALSUP_INPUT_SCHEMA)

    dialsup = metric_long.filter(
        (pl.col(Col.is_dialsup) == 1) & (pl.col(Col.metric) == "forecast")
    ).with_columns(pl.lit("dialsup_gbp_forecast").alias(Col.metric))
    require_columns(dialsup, DIALSUP_SCHEMA)
    return dialsup
