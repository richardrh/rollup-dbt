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

MODEL = "int_ylt_main_forecast"


def validate(
    ylt_localccy: pl.LazyFrame,
    forecast_dates: pl.LazyFrame,
    forecast_factors: pl.LazyFrame,
) -> None:
    local_schema = collect_lazy_schema(MODEL, "ylt_localccy", ylt_localccy)
    dates_schema = collect_lazy_schema(MODEL, "forecast_dates", forecast_dates)
    factor_schema = collect_lazy_schema(MODEL, "forecast_factors", forecast_factors)
    require_columns(
        MODEL,
        "ylt_localccy",
        local_schema,
        [Col.class_, Col.office, Col.loss],
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
        "ylt_localccy",
        local_schema,
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
    ylt_localccy: pl.LazyFrame,
    forecast_dates: pl.LazyFrame,
    forecast_factors: pl.LazyFrame,
) -> pl.LazyFrame:
    validate(ylt_localccy, forecast_dates, forecast_factors)
    frame = (
        ylt_localccy.join(forecast_dates, how="cross")
        .join(
            forecast_factors, on=[Col.class_, Col.office, Col.forecast_date], how="left"
        )
        .with_columns(
            (pl.col(Col.loss) * pl.col("_forecast_factor_raw").fill_null(1.0)).alias(
                Col.loss
            ),
            pl.lit("localccy_forecast").alias(Col.metric),
        )
        .drop("_forecast_factor_raw")
    )
    validate_output(MODEL, frame)
    return frame
