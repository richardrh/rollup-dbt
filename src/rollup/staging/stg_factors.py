from __future__ import annotations
# mypy: ignore-errors

import polars as pl

from rollup.columns import Col, RawCol


def stg_gbp_fx_rates(fx_rates: pl.LazyFrame) -> pl.LazyFrame:
    return fx_rates.filter(pl.col(Col.target_currency) == "GBP").select(
        pl.col(RawCol.currency_code).alias(Col.currency),
        Col.target_currency,
        pl.col(RawCol.rate_date).alias(Col.fx_rate_date),
        pl.col(RawCol.rate).alias(Col.fx_rate),
    )


def stg_forecast_factors(forecast_factors: pl.LazyFrame) -> pl.LazyFrame:
    return forecast_factors.select(
        Col.class_,
        pl.col("office_iso2").alias(Col.office),
        Col.forecast_date,
        pl.col(RawCol.factor).alias("_forecast_factor_raw"),
    )


def stg_forecast_dates(forecast_factors: pl.LazyFrame) -> pl.LazyFrame:
    return forecast_factors.select(Col.forecast_date).unique()
