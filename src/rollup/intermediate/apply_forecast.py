from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol
from rollup.intermediate.apply_fx import FX_APPLIED_YLT_SCHEMA


FORECAST_INPUT_SCHEMA = FX_APPLIED_YLT_SCHEMA
FORECAST_FACTORS_SCHEMA = pl.Schema(
    {
        Col.class_: pl.String,
        Col.office: pl.String,
        Col.forecast_date: pl.String,
        RawCol.factor: pl.Float64,
    }
)
FORECAST_APPLIED_YLT_SCHEMA = pl.Schema(
    {
        **FORECAST_INPUT_SCHEMA,
        Col.forecast_date: pl.String,
        Col.forecast_factor: pl.Float64,
        "forecast_loss": pl.Float64,
    }
)


def apply_forecast(frame: pl.LazyFrame, forecast_factors: pl.DataFrame) -> pl.LazyFrame:
    actual = frame.collect_schema()
    missing = [str(name) for name in FORECAST_INPUT_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"apply_forecast missing columns: {missing}")

    if forecast_factors.is_empty():
        applied = frame.with_columns(
            pl.lit("base").alias(Col.forecast_date),
            pl.lit(1.0).alias(Col.forecast_factor),
            pl.col("gbp_loss").alias("forecast_loss"),
        )
        actual = applied.collect_schema()
        missing = [str(name) for name in FORECAST_APPLIED_YLT_SCHEMA if name not in actual]
        if missing:
            raise ValueError(f"apply_forecast missing columns: {missing}")
        return applied
    actual = forecast_factors.schema
    missing = [str(name) for name in FORECAST_FACTORS_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"apply_forecast missing columns: {missing}")
    factors = forecast_factors.lazy().select(
        pl.col(Col.class_).cast(pl.String),
        pl.col(Col.office).cast(pl.String),
        pl.col(Col.forecast_date).cast(pl.String),
        pl.col(RawCol.factor).cast(pl.Float64).alias(Col.forecast_factor),
    )
    applied = frame.join(factors, on=[Col.class_, Col.office], how="left").with_columns(
        pl.col(Col.forecast_date).fill_null("base"),
        pl.col(Col.forecast_factor).fill_null(1.0),
    ).with_columns((pl.col("gbp_loss") * pl.col(Col.forecast_factor)).alias("forecast_loss"))
    actual = applied.collect_schema()
    missing = [str(name) for name in FORECAST_APPLIED_YLT_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"apply_forecast missing columns: {missing}")
    return applied
