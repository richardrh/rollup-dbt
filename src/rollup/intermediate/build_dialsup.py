from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.intermediate.build_metric_long import METRIC_LONG_SCHEMA
from rollup.metric_names import loss_blended_fx_forecast_metric, loss_dialsup_fx_forecast_metric


DIALSUP_INPUT_SCHEMA = METRIC_LONG_SCHEMA
DIALSUP_SCHEMA = METRIC_LONG_SCHEMA


def build_dialsup(metric_long: pl.LazyFrame, target_currency: str = "GBP") -> pl.LazyFrame:
    DIALSUP_INPUT_SCHEMA.validate(metric_long)

    dialsup = metric_long.filter(
        (pl.col(Col.is_dialsup) == 1)
        & (pl.col(Col.metric) == loss_blended_fx_forecast_metric(target_currency))
    ).with_columns(pl.lit(loss_dialsup_fx_forecast_metric(target_currency)).alias(Col.metric))
    return dialsup
