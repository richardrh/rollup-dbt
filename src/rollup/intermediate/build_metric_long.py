from __future__ import annotations

import polars as pl
import pandera.polars as pa

from rollup.columns import Col
from rollup.intermediate.apply_euws import EUWS_APPLIED_YLT_SCHEMA
from rollup.metric_names import (
    LOSS_BLENDED,
    LOSS_ORIGINAL_YLT,
    loss_blended_fx_forecast_euws_override_metric,
    loss_blended_fx_forecast_metric,
    loss_blended_fx_metric,
    normalize_target_currency,
)


METRIC_LONG_INPUT_SCHEMA = EUWS_APPLIED_YLT_SCHEMA
METRIC_LONG_SCHEMA = pa.DataFrameSchema(
    {
        Col.vendor: pa.Column(pl.String, nullable=True),
        Col.base_model: pa.Column(pl.String, nullable=True),
        Col.analysis_id: pa.Column(pl.String, nullable=True),
        Col.modelled_lob: pa.Column(pl.String, nullable=True),
        Col.modelled_peril: pa.Column(pl.String, nullable=True),
        Col.rollup_lob: pa.Column(pl.String, nullable=True),
        Col.rollup_peril: pa.Column(pl.String, nullable=True),
        Col.region_peril_id: pa.Column(pl.Int64, nullable=True),
        Col.class_: pa.Column(pl.String, nullable=True),
        Col.office: pa.Column(pl.String, nullable=True),
        Col.currency: pa.Column(pl.String, nullable=True),
        Col.target_currency: pa.Column(pl.String, nullable=True),
        Col.year_id: pa.Column(pl.Int64, nullable=True),
        Col.event_id: pa.Column(pl.Int64, nullable=True),
        Col.forecast_date: pa.Column(pl.String, nullable=True),
        Col.is_dialsup: pa.Column(pl.Int64, nullable=True),
        Col.metric: pa.Column(pl.String, nullable=True),
        Col.loss: pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)


def build_metric_long(adjusted: pl.LazyFrame, target_currency: str = "GBP") -> pl.LazyFrame:
    METRIC_LONG_INPUT_SCHEMA.validate(adjusted)
    target_currency = normalize_target_currency(target_currency)

    metric_columns = [
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
    ]

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
        Col.target_currency,
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
            base.select(
                *metric_columns,
                pl.lit(LOSS_ORIGINAL_YLT).alias(Col.metric),
                pl.col(Col.loss).cast(pl.Float64).alias(Col.loss),
            ),
            base.select(
                *metric_columns,
                pl.lit(LOSS_BLENDED).alias(Col.metric),
                pl.col("blended_loss").cast(pl.Float64).alias(Col.loss),
            ),
            base.select(
                *metric_columns,
                pl.lit(loss_blended_fx_metric(target_currency)).alias(Col.metric),
                pl.col("gbp_loss").cast(pl.Float64).alias(Col.loss),
            ),
            base.select(
                *metric_columns,
                pl.lit(loss_blended_fx_forecast_metric(target_currency)).alias(Col.metric),
                pl.col("forecast_loss").cast(pl.Float64).alias(Col.loss),
            ),
            base.select(
                *metric_columns,
                pl.lit(loss_blended_fx_forecast_euws_override_metric(target_currency)).alias(Col.metric),
                pl.col("euws_loss").cast(pl.Float64).alias(Col.loss),
            ),
        ],
        how="vertical",
    )
    return metric_long
