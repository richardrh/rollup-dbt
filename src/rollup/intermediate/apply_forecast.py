from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol


def apply_forecast(frame: pl.LazyFrame, forecast_factors: pl.DataFrame) -> pl.LazyFrame:
    if forecast_factors.is_empty():
        return frame.with_columns(
            pl.lit("base").alias(Col.forecast_date),
            pl.lit(1.0).alias(Col.forecast_factor),
            pl.col("gbp_loss").alias("forecast_loss"),
        )
    factors = forecast_factors.lazy().select(
        pl.col(Col.class_).cast(pl.String),
        pl.col(Col.office).cast(pl.String),
        pl.col(Col.forecast_date).cast(pl.String),
        pl.col(RawCol.factor).cast(pl.Float64).alias(Col.forecast_factor),
    )
    return frame.join(factors, on=[Col.class_, Col.office], how="left").with_columns(
        pl.col(Col.forecast_date).fill_null("base"),
        pl.col(Col.forecast_factor).fill_null(1.0),
    ).with_columns((pl.col("gbp_loss") * pl.col(Col.forecast_factor)).alias("forecast_loss"))
