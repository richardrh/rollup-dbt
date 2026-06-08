from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.intermediate.apply_euws import EUWS_APPLIED_YLT_SCHEMA
from rollup.schemas import require_columns


METRIC_LONG_INPUT_SCHEMA = EUWS_APPLIED_YLT_SCHEMA
METRIC_LONG_SCHEMA = pl.Schema(
    {
        Col.vendor: pl.String,
        Col.base_model: pl.String,
        Col.analysis_id: pl.String,
        Col.modelled_lob: pl.String,
        Col.modelled_peril: pl.String,
        Col.rollup_lob: pl.String,
        Col.rollup_peril: pl.String,
        Col.region_peril_id: pl.Int64,
        Col.class_: pl.String,
        Col.office: pl.String,
        Col.currency: pl.String,
        Col.year_id: pl.Int64,
        Col.event_id: pl.Int64,
        Col.forecast_date: pl.String,
        Col.is_dialsup: pl.Int64,
        Col.metric: pl.String,
        Col.loss: pl.Float64,
    }
)


def build_metric_long(adjusted: pl.LazyFrame) -> pl.LazyFrame:
    require_columns(adjusted, METRIC_LONG_INPUT_SCHEMA)

    base = adjusted.select(
        Col.vendor,
        pl.col(Col.vendor).alias(Col.base_model),
        Col.analysis_id,
        Col.modelled_lob,
        Col.modelled_peril,
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.class_,
        Col.office,
        Col.currency,
        Col.year_id,
        Col.event_id,
        Col.forecast_date,
        Col.is_dialsup,
        Col.loss,
        "blended_loss",
        "gbp_loss",
        "forecast_loss",
        "euws_loss",
    )
    metric_long = pl.concat(
        [
            _metric(base, Col.loss, "original_ylt_loss"),
            _metric(base, "blended_loss", "blended"),
            _metric(base, "gbp_loss", "gbp"),
            _metric(base, "forecast_loss", "forecast"),
            _metric(base, "euws_loss", "euws_override"),
        ],
        how="vertical",
    )
    require_columns(metric_long, METRIC_LONG_SCHEMA)
    return metric_long


def _metric(frame: pl.LazyFrame, source_col: str, metric: str) -> pl.LazyFrame:
    return frame.select(
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
        Col.year_id,
        Col.event_id,
        Col.forecast_date,
        Col.is_dialsup,
        pl.lit(metric).alias(Col.metric),
        pl.col(source_col).cast(pl.Float64).alias(Col.loss),
    )
