from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
)

MODEL = "stg_forecast_factors"


def validate(forecast_factors: pl.LazyFrame) -> None:
    schema = collect_lazy_schema(MODEL, "forecast_factors", forecast_factors)
    require_columns(
        MODEL,
        "forecast_factors",
        schema,
        [Col.class_, "office_iso2", Col.forecast_date],
    )
    require_dtype_family(MODEL, "forecast_factors", schema, RawCol.factor, "numeric")


def transform(forecast_factors: pl.LazyFrame) -> pl.LazyFrame:
    validate(forecast_factors)
    frame = forecast_factors.select(
        Col.class_,
        pl.col("office_iso2").alias(Col.office),
        pl.col(Col.forecast_date)
        .cast(pl.String)
        .str.to_date("%Y-%m-%d", strict=True)
        .alias(Col.forecast_date),
        pl.col(RawCol.factor).alias("_forecast_factor_raw"),
    )
    validate_output(MODEL, frame)
    return frame
