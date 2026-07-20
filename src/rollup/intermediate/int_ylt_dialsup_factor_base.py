from __future__ import annotations

import polars as pl
from rollup.model_validation import validate_schema

from rollup.columns import Col

MODEL = "int_ylt_dialsup_factor_base"


def schema() -> pl.Schema:
    return pl.Schema(
        {
            Col.vendor: pl.String,
            Col.analysis_id: pl.String,
            Col.modelled_lob: pl.String,
            Col.modelled_peril: pl.String,
            Col.rollup_lob: pl.String,
            Col.rollup_peril: pl.String,
            Col.region_peril_id: pl.Int64,
            Col.blend_subregion_peril_id: pl.String,
            Col.base_model: pl.String,
            Col.selection_priority: pl.Int64,
            Col.is_dialsup: pl.Int64,
            Col.is_euws: pl.Int64,
            Col.cds_cat_class_name: pl.String,
            Col.class_: pl.String,
            Col.office: pl.String,
            Col.currency: pl.String,
            Col.model_code: pl.Int64,
            Col.year_id: pl.Int64,
            Col.event_id: pl.Int64,
            Col.loss: pl.Float64,
            Col.metric: pl.String,
            Col.rnk: pl.Int64,
            Col.rp: pl.Float64,
            Col.rp_bucket: pl.Int32,
            Col.model_event_id: pl.Int64,
            Col.event_day: pl.Int64,
            Col.target_currency: pl.String,
            Col.fx_rate_date: pl.String,
            Col.fx_rate: pl.Float64,
            Col.forecast_date: pl.Date,
            "_forecast_factor_raw": pl.Float64,
            "_forecast_factor": pl.Float64,
        }
    )  # type: ignore[arg-type]


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(
    ylt: pl.LazyFrame,
    verisk_events: pl.LazyFrame,
    fx_rates: pl.LazyFrame,
    forecast_dates: pl.LazyFrame,
    forecast_factors: pl.LazyFrame,
) -> pl.LazyFrame:
    frame = (
        ylt.join(
            verisk_events, on=[Col.event_id, Col.year_id, Col.model_code], how="left"
        )
        .join(fx_rates, on=Col.currency, how="inner")
        .join(forecast_dates, how="cross")
        .join(
            forecast_factors, on=[Col.class_, Col.office, Col.forecast_date], how="left"
        )
        .with_columns(
            pl.col("_forecast_factor_raw").fill_null(1.0).alias("_forecast_factor")
        )
        .select(
            Col.vendor,
            Col.analysis_id,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.rollup_lob,
            Col.rollup_peril,
            pl.col(Col.region_peril_id).cast(pl.Int64),
            Col.blend_subregion_peril_id,
            Col.base_model,
            pl.col(Col.selection_priority).cast(pl.Int64),
            pl.col(Col.is_dialsup).cast(pl.Int64),
            pl.col(Col.is_euws).cast(pl.Int64),
            Col.cds_cat_class_name,
            Col.class_,
            Col.office,
            Col.currency,
            pl.col(Col.model_code).cast(pl.Int64),
            pl.col(Col.year_id).cast(pl.Int64),
            pl.col(Col.event_id).cast(pl.Int64),
            pl.col(Col.loss).cast(pl.Float64),
            Col.metric,
            pl.col(Col.rnk).cast(pl.Int64),
            pl.col(Col.rp).cast(pl.Float64),
            pl.col(Col.rp_bucket).cast(pl.Int32),
            pl.col(Col.model_event_id).cast(pl.Int64),
            pl.col(Col.event_day).cast(pl.Int64),
            Col.target_currency,
            Col.fx_rate_date,
            pl.col(Col.fx_rate).cast(pl.Float64),
            pl.col(Col.forecast_date).cast(pl.Date),
            pl.col("_forecast_factor_raw").cast(pl.Float64),
            pl.col("_forecast_factor").cast(pl.Float64),
        )
    )
    validate(frame)
    return frame
