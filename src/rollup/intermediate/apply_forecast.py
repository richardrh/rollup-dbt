from __future__ import annotations

import polars as pl
import pandera.polars as pa

from rollup.columns import Col, RawCol
from rollup.intermediate.apply_fx import FX_APPLIED_YLT_SCHEMA


FORECAST_INPUT_SCHEMA = FX_APPLIED_YLT_SCHEMA
FORECAST_FACTORS_SCHEMA = pa.DataFrameSchema(
    {
        Col.class_: pa.Column(pl.String, nullable=True),
        Col.office: pa.Column(pl.String, nullable=True),
        Col.forecast_date: pa.Column(pl.String, nullable=True),
        RawCol.factor: pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)
FORECAST_APPLIED_YLT_SCHEMA = pa.DataFrameSchema(
    {
        **FORECAST_INPUT_SCHEMA.columns,
        Col.forecast_date: pa.Column(pl.String, nullable=True),
        Col.forecast_factor: pa.Column(pl.Float64, nullable=True),
        "forecast_loss": pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)


def apply_forecast(frame: pl.LazyFrame, forecast_factors: pl.DataFrame) -> pl.LazyFrame:
    FORECAST_INPUT_SCHEMA.validate(frame)

    if forecast_factors.is_empty():
        return frame.with_columns(
            pl.lit("base").alias(Col.forecast_date),
            pl.lit(1.0).alias(Col.forecast_factor),
            pl.col("fx_loss").alias("forecast_loss"),
        )
    FORECAST_FACTORS_SCHEMA.validate(forecast_factors)
    factors = forecast_factors.lazy().select(
        pl.col(Col.class_).cast(pl.String),
        pl.col(Col.office).cast(pl.String),
        pl.col(Col.forecast_date).cast(pl.String),
        pl.col(RawCol.factor).cast(pl.Float64).alias(Col.forecast_factor),
    )
    applied = frame.join(factors, on=[Col.class_, Col.office], how="left").with_columns(
        pl.col(Col.forecast_date).fill_null("base"),
        pl.col(Col.forecast_factor).fill_null(1.0),
    ).with_columns((pl.col("fx_loss") * pl.col(Col.forecast_factor)).alias("forecast_loss"))
    return applied
