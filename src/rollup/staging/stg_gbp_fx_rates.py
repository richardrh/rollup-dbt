from __future__ import annotations

import polars as pl
from rollup.model_validation import validate_schema

from rollup.columns import Col, RawCol

MODEL = "stg_gbp_fx_rates"


def schema() -> pl.Schema:
    return pl.Schema(
        {  # type: ignore[arg-type]  # Polars accepts StrEnum keys.
            Col.currency: pl.String,
            Col.target_currency: pl.String,
            Col.fx_rate_date: pl.String,
            Col.fx_rate: pl.Float64,
        }
    )


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(fx_rates: pl.LazyFrame) -> pl.LazyFrame:
    frame = fx_rates.filter(pl.col(Col.target_currency) == "GBP").select(
        pl.col(RawCol.currency_code).cast(pl.String).alias(Col.currency),
        pl.col(Col.target_currency).cast(pl.String),
        pl.col(RawCol.rate_date).cast(pl.String).alias(Col.fx_rate_date),
        pl.col(RawCol.rate).cast(pl.Float64).alias(Col.fx_rate),
    )
    validate(frame)
    return frame
