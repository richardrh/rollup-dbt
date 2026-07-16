from __future__ import annotations
import polars as pl
from rollup.columns import Col
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
    require_join_key_compatible,
)

MODEL = "int_ylt_dialsup_factor_base"


def validate(
    ylt: pl.LazyFrame,
    verisk_events: pl.LazyFrame,
    fx_rates: pl.LazyFrame,
    forecast_dates: pl.LazyFrame,
    forecast_factors: pl.LazyFrame,
) -> None:
    ylt_schema = collect_lazy_schema(MODEL, "ylt", ylt)
    events_schema = collect_lazy_schema(MODEL, "verisk_events", verisk_events)
    fx_schema = collect_lazy_schema(MODEL, "fx_rates", fx_rates)
    dates_schema = collect_lazy_schema(MODEL, "forecast_dates", forecast_dates)
    factor_schema = collect_lazy_schema(MODEL, "forecast_factors", forecast_factors)
    require_columns(
        MODEL,
        "ylt",
        ylt_schema,
        [
            Col.event_id,
            Col.year_id,
            Col.model_code,
            Col.currency,
            Col.class_,
            Col.office,
        ],
    )
    require_columns(
        MODEL,
        "verisk_events",
        events_schema,
        [Col.event_id, Col.year_id, Col.model_code],
    )
    require_columns(
        MODEL,
        "fx_rates",
        fx_schema,
        [Col.currency, Col.fx_rate],
    )
    require_columns(
        MODEL,
        "forecast_dates",
        dates_schema,
        [Col.forecast_date],
    )
    require_columns(
        MODEL,
        "forecast_factors",
        factor_schema,
        [Col.class_, Col.office, Col.forecast_date, "_forecast_factor_raw"],
    )
    require_dtype_family(MODEL, "fx_rates", fx_schema, Col.fx_rate, "numeric")
    require_dtype_family(
        MODEL, "forecast_factors", factor_schema, "_forecast_factor_raw", "numeric"
    )
    require_dtype_family(
        MODEL, "forecast_dates", dates_schema, Col.forecast_date, "date_like"
    )
    require_dtype_family(
        MODEL, "forecast_factors", factor_schema, Col.forecast_date, "date_like"
    )
    require_join_key_compatible(
        MODEL,
        "ylt",
        ylt_schema,
        "verisk_events",
        events_schema,
        [Col.event_id, Col.year_id, Col.model_code],
    )
    require_join_key_compatible(
        MODEL, "ylt", ylt_schema, "fx_rates", fx_schema, [Col.currency]
    )
    require_join_key_compatible(
        MODEL,
        "ylt",
        ylt_schema,
        "forecast_factors",
        factor_schema,
        [Col.class_, Col.office],
    )
    require_join_key_compatible(
        MODEL,
        "forecast_dates",
        dates_schema,
        "forecast_factors",
        factor_schema,
        [Col.forecast_date],
    )


def transform(
    ylt: pl.LazyFrame,
    verisk_events: pl.LazyFrame,
    fx_rates: pl.LazyFrame,
    forecast_dates: pl.LazyFrame,
    forecast_factors: pl.LazyFrame,
) -> pl.LazyFrame:
    validate(ylt, verisk_events, fx_rates, forecast_dates, forecast_factors)
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
    )
    validate_output(MODEL, frame)
    return frame
