from __future__ import annotations

import polars as pl
import pandera.polars as pa

from rollup.columns import Col, RawCol
from rollup.intermediate.apply_blending import BLENDED_YLT_SCHEMA
from rollup.metric_names import normalize_target_currency


FX_SOURCE_CURRENCY = "__fx_source_currency"


FX_INPUT_SCHEMA = BLENDED_YLT_SCHEMA
FX_RATES_SCHEMA = pa.DataFrameSchema(
    {
        Col.currency: pa.Column(pl.String, nullable=True),
        Col.target_currency: pa.Column(pl.String, nullable=True),
        RawCol.rate: pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)
RAW_FX_RATES_SCHEMA = pa.DataFrameSchema(
    {
        RawCol.currency_code: pa.Column(pl.String, nullable=True),
        Col.target_currency: pa.Column(pl.String, nullable=True),
        RawCol.rate: pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)
FX_APPLIED_YLT_SCHEMA = pa.DataFrameSchema(
    {
        **FX_INPUT_SCHEMA.columns,
        Col.fx_rate: pa.Column(pl.Float64, nullable=True),
        Col.target_currency: pa.Column(pl.String, nullable=True),
        "fx_loss": pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)


def apply_fx(frame: pl.LazyFrame, fx_rates: pl.DataFrame, target_currency: str = "GBP") -> pl.LazyFrame:
    FX_INPUT_SCHEMA.validate(frame)
    target_currency = normalize_target_currency(target_currency)
    source_currencies = collect_source_currencies(frame)

    if fx_rates.is_empty():
        validate_missing_fx_rates(source_currencies, (), target_currency)
        return frame.with_columns(
            pl.lit(1.0).alias(Col.fx_rate),
            pl.lit(target_currency).alias(Col.target_currency),
            pl.col("blended_loss").alias("fx_loss"),
        )
    columns = fx_rates.columns
    if Col.target_currency not in columns:
        raise ValueError("FX rates must include target_currency column")
    currency_col = RawCol.currency_code if RawCol.currency_code in columns else Col.currency
    rates_schema = RAW_FX_RATES_SCHEMA if RawCol.currency_code in columns else FX_RATES_SCHEMA
    rates_schema.validate(fx_rates)
    rates = fx_rates.lazy().select(
        pl.col(currency_col).cast(pl.String).str.to_uppercase().alias(FX_SOURCE_CURRENCY),
        pl.col(Col.target_currency).cast(pl.String).str.to_uppercase().alias(Col.target_currency),
        pl.col(RawCol.rate).cast(pl.Float64).alias(Col.fx_rate),
    ).filter(
        pl.col(Col.target_currency) == target_currency
    ).filter(
        pl.col(FX_SOURCE_CURRENCY).is_not_null() & pl.col(Col.fx_rate).is_not_null()
    ).unique(FX_SOURCE_CURRENCY, keep="last")
    rate_currencies = rates.select(FX_SOURCE_CURRENCY).collect().to_series().to_list()
    validate_missing_fx_rates(source_currencies, rate_currencies, target_currency)
    applied = frame.with_columns(
        pl.col(Col.currency).cast(pl.String).str.to_uppercase().alias(FX_SOURCE_CURRENCY)
    ).join(
        rates,
        on=FX_SOURCE_CURRENCY,
        how="left",
    ).with_columns(
        pl.when(pl.col(Col.fx_rate).is_null() & (pl.col(FX_SOURCE_CURRENCY) == target_currency))
        .then(1.0)
        .otherwise(pl.col(Col.fx_rate))
        .alias(Col.fx_rate),
        pl.lit(target_currency).alias(Col.target_currency),
    ).drop(FX_SOURCE_CURRENCY).with_columns(
        (pl.col("blended_loss") * pl.col(Col.fx_rate)).alias("fx_loss")
    )
    return applied


def collect_source_currencies(frame: pl.LazyFrame) -> tuple[str, ...]:
    return tuple(
        sorted(
            currency
            for currency in frame.select(
                pl.col(Col.currency).cast(pl.String).str.to_uppercase().alias(Col.currency)
            ).unique().collect().to_series().to_list()
            if currency is not None
        )
    )


def validate_missing_fx_rates(
    source_currencies: tuple[str, ...],
    rate_currencies: tuple[str, ...] | list[str],
    target_currency: str,
) -> None:
    available = set(rate_currencies)
    missing = [currency for currency in source_currencies if currency != target_currency and currency not in available]
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(f"missing FX rates for currencies {missing_list} targeting {target_currency}")
