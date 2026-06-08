from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol


def apply_fx(frame: pl.LazyFrame, fx_rates: pl.DataFrame) -> pl.LazyFrame:
    if fx_rates.is_empty():
        return frame.with_columns(pl.lit(1.0).alias(Col.fx_rate), pl.col("blended_loss").alias("gbp_loss"))
    columns = fx_rates.columns
    currency_col = RawCol.currency_code if RawCol.currency_code in columns else Col.currency
    rates = fx_rates.lazy().select(
        pl.col(currency_col).cast(pl.String).alias(Col.currency),
        pl.col(RawCol.rate).cast(pl.Float64).alias(Col.fx_rate),
    ).unique(Col.currency, keep="last")
    return frame.join(rates, on=Col.currency, how="left").with_columns(
        pl.col(Col.fx_rate).fill_null(1.0),
    ).with_columns((pl.col("blended_loss") * pl.col(Col.fx_rate)).alias("gbp_loss"))
