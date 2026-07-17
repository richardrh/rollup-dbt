from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
)

MODEL = "stg_gbp_fx_rates"


def validate(fx_rates: pl.LazyFrame) -> None:
    schema = collect_lazy_schema(MODEL, "fx_rates", fx_rates)
    require_columns(
        MODEL,
        "fx_rates",
        schema,
        [Col.target_currency, RawCol.currency_code, RawCol.rate_date],
    )
    require_dtype_family(MODEL, "fx_rates", schema, RawCol.rate, "numeric")


def transform(fx_rates: pl.LazyFrame) -> pl.LazyFrame:
    validate(fx_rates)
    frame = fx_rates.filter(pl.col(Col.target_currency) == "GBP").select(
        pl.col(RawCol.currency_code).alias(Col.currency),
        Col.target_currency,
        pl.col(RawCol.rate_date).alias(Col.fx_rate_date),
        pl.col(RawCol.rate).alias(Col.fx_rate),
    )
    validate_output(MODEL, frame)
    return frame
