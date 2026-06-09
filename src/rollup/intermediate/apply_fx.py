from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol
from rollup.intermediate.apply_blending import BLENDED_YLT_SCHEMA


FX_INPUT_SCHEMA = BLENDED_YLT_SCHEMA
FX_RATES_SCHEMA = pl.Schema(
    {
        Col.currency: pl.String,
        RawCol.rate: pl.Float64,
    }
)
RAW_FX_RATES_SCHEMA = pl.Schema(
    {
        RawCol.currency_code: pl.String,
        RawCol.rate: pl.Float64,
    }
)
FX_APPLIED_YLT_SCHEMA = pl.Schema(
    {
        **FX_INPUT_SCHEMA,
        Col.fx_rate: pl.Float64,
        "gbp_loss": pl.Float64,
    }
)


def apply_fx(frame: pl.LazyFrame, fx_rates: pl.DataFrame) -> pl.LazyFrame:
    actual = frame.collect_schema()
    missing = [str(name) for name in FX_INPUT_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"apply_fx missing columns: {missing}")

    if fx_rates.is_empty():
        applied = frame.with_columns(
            pl.lit(1.0).alias(Col.fx_rate),
            pl.col("blended_loss").alias("gbp_loss"),
        )
        actual = applied.collect_schema()
        missing = [str(name) for name in FX_APPLIED_YLT_SCHEMA if name not in actual]
        if missing:
            raise ValueError(f"apply_fx missing columns: {missing}")
        return applied
    columns = fx_rates.columns
    currency_col = RawCol.currency_code if RawCol.currency_code in columns else Col.currency
    rates_schema = RAW_FX_RATES_SCHEMA if RawCol.currency_code in columns else FX_RATES_SCHEMA
    actual = fx_rates.schema
    missing = [str(name) for name in rates_schema if name not in actual]
    if missing:
        raise ValueError(f"apply_fx missing columns: {missing}")
    rates = fx_rates.lazy().select(
        pl.col(currency_col).cast(pl.String).alias(Col.currency),
        pl.col(RawCol.rate).cast(pl.Float64).alias(Col.fx_rate),
    ).unique(Col.currency, keep="last")
    applied = frame.join(rates, on=Col.currency, how="left").with_columns(
        pl.col(Col.fx_rate).fill_null(1.0),
    ).with_columns((pl.col("blended_loss") * pl.col(Col.fx_rate)).alias("gbp_loss"))
    actual = applied.collect_schema()
    missing = [str(name) for name in FX_APPLIED_YLT_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"apply_fx missing columns: {missing}")
    return applied
