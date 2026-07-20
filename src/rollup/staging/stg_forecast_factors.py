from __future__ import annotations

import polars as pl
from rollup.model_validation import validate_schema

from rollup.columns import Col, RawCol

MODEL = "stg_forecast_factors"


def schema() -> pl.Schema:
    return pl.Schema(
        {
            Col.class_: pl.String,
            Col.office: pl.String,
            Col.forecast_date: pl.Date,
            "_forecast_factor_raw": pl.Float64,
        }
    )


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(forecast_factors: pl.LazyFrame) -> pl.LazyFrame:
    frame = forecast_factors.select(
        Col.class_,
        pl.col("office_iso2").alias(Col.office),
        pl.col(Col.forecast_date)
        .cast(pl.String)
        .str.to_date("%Y-%m-%d", strict=True)
        .alias(Col.forecast_date),
        pl.col(RawCol.factor).alias("_forecast_factor_raw"),
    )
    validate(frame)
    return frame
