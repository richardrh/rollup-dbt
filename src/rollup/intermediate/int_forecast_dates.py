from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
)

MODEL = "int_forecast_dates"


def validate(forecast_factors: pl.LazyFrame) -> None:
    schema = collect_lazy_schema(MODEL, "forecast_factors", forecast_factors)
    require_columns(MODEL, "forecast_factors", schema, [Col.forecast_date])
    require_dtype_family(
        MODEL, "forecast_factors", schema, Col.forecast_date, "date_like"
    )


def transform(forecast_factors: pl.LazyFrame) -> pl.LazyFrame:
    validate(forecast_factors)
    frame = forecast_factors.select(Col.forecast_date).unique()
    validate_output(MODEL, frame)
    return frame
