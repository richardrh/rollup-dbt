from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.intermediate.apply_euws import EUWS_APPLIED_YLT_SCHEMA


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
    actual = adjusted.collect_schema()
    missing = [str(name) for name in METRIC_LONG_INPUT_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"build_metric_long missing columns: {missing}")

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
                pl.lit("original_ylt_loss").alias(Col.metric),
                pl.col(Col.loss).cast(pl.Float64).alias(Col.loss),
            ),
            base.select(
                *metric_columns,
                pl.lit("blended").alias(Col.metric),
                pl.col("blended_loss").cast(pl.Float64).alias(Col.loss),
            ),
            base.select(
                *metric_columns,
                pl.lit("gbp").alias(Col.metric),
                pl.col("gbp_loss").cast(pl.Float64).alias(Col.loss),
            ),
            base.select(
                *metric_columns,
                pl.lit("forecast").alias(Col.metric),
                pl.col("forecast_loss").cast(pl.Float64).alias(Col.loss),
            ),
            base.select(
                *metric_columns,
                pl.lit("euws_override").alias(Col.metric),
                pl.col("euws_loss").cast(pl.Float64).alias(Col.loss),
            ),
        ],
        how="vertical",
    )
    actual = metric_long.collect_schema()
    missing = [str(name) for name in METRIC_LONG_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"build_metric_long missing columns: {missing}")
    return metric_long
