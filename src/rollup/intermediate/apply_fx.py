from __future__ import annotations

import polars as pl
import pandera.polars as pa

from rollup.columns import Col, RawCol
from rollup.intermediate.apply_blending import BLENDED_YLT_SCHEMA


FX_INPUT_SCHEMA = BLENDED_YLT_SCHEMA
FX_RATES_SCHEMA = pa.DataFrameSchema(
    {
        Col.currency: pa.Column(pl.String, nullable=True),
        RawCol.rate: pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)
RAW_FX_RATES_SCHEMA = pa.DataFrameSchema(
    {
        RawCol.currency_code: pa.Column(pl.String, nullable=True),
        RawCol.rate: pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)
FX_APPLIED_YLT_SCHEMA = pa.DataFrameSchema(
    {
        **FX_INPUT_SCHEMA.columns,
        Col.fx_rate: pa.Column(pl.Float64, nullable=True),
        "gbp_loss": pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)


def apply_fx(frame: pl.LazyFrame, fx_rates: pl.DataFrame) -> pl.LazyFrame:
    FX_INPUT_SCHEMA.validate(frame)

    if fx_rates.is_empty():
        return frame.with_columns(
            pl.lit(1.0).alias(Col.fx_rate),
            pl.col("blended_loss").alias("gbp_loss"),
        )
    columns = fx_rates.columns
    currency_col = RawCol.currency_code if RawCol.currency_code in columns else Col.currency
    rates_schema = RAW_FX_RATES_SCHEMA if RawCol.currency_code in columns else FX_RATES_SCHEMA
    rates_schema.validate(fx_rates)
    rates = fx_rates.lazy().select(
        pl.col(currency_col).cast(pl.String).alias(Col.currency),
        pl.col(RawCol.rate).cast(pl.Float64).alias(Col.fx_rate),
    ).unique(Col.currency, keep="last")
    applied = frame.join(rates, on=Col.currency, how="left").with_columns(
        pl.col(Col.fx_rate).fill_null(1.0),
    ).with_columns((pl.col("blended_loss") * pl.col(Col.fx_rate)).alias("gbp_loss"))
    return applied
