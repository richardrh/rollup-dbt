from __future__ import annotations

import polars as pl
from rollup.model_validation import validate_schema

from rollup.columns import Col

MODEL = "int_forecast_dates"


def schema() -> pl.Schema:
    return pl.Schema({Col.forecast_date: pl.Date})  # type: ignore[arg-type]  # Polars accepts StrEnum keys.


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(forecast_factors: pl.LazyFrame) -> pl.LazyFrame:
    frame = (
        forecast_factors.select(pl.col(Col.forecast_date).cast(pl.Date))
        .unique()
        .select(pl.col(Col.forecast_date).cast(pl.Date))
    )
    validate(frame)
    return frame
