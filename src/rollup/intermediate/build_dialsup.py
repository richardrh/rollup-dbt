from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.intermediate.apply_euws import EUWS_APPLIED_YLT_SCHEMA
from rollup.intermediate.build_metric_long import METRIC_LONG_SCHEMA
from rollup.metric_names import loss_dialsup_fx_forecast_metric


DIALSUP_INPUT_SCHEMA = EUWS_APPLIED_YLT_SCHEMA
DIALSUP_SCHEMA = METRIC_LONG_SCHEMA


def build_dialsup(adjusted: pl.LazyFrame, target_currency: str = "GBP") -> pl.LazyFrame:
    DIALSUP_INPUT_SCHEMA.validate(adjusted)

    return adjusted.filter(pl.col(Col.is_dialsup) == 1).select(
        Col.vendor,
        Col.base_model,
        Col.analysis_id,
        Col.modelled_lob,
        Col.modelled_peril,
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.class_,
        Col.office,
        Col.currency,
        Col.target_currency,
        Col.year_id,
        Col.event_id,
        Col.forecast_date,
        Col.is_dialsup,
        pl.lit(loss_dialsup_fx_forecast_metric(target_currency)).alias(Col.metric),
        (pl.col(Col.loss) * pl.col(Col.fx_rate) * pl.col(Col.forecast_factor))
        .cast(pl.Float64)
        .alias(Col.loss),
    )
