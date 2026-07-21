from __future__ import annotations

from typing import override

import polars as pl

from rollup.columns import Col, RawCol
from rollup.model import PolarsModel


class Model(PolarsModel[[pl.LazyFrame]]):
    @override
    @classmethod
    def schema(cls) -> pl.Schema:
        return pl.Schema(
            {  # type: ignore[arg-type]  # Polars accepts StrEnum keys.
                Col.currency: pl.String,
                Col.target_currency: pl.String,
                Col.fx_rate_date: pl.String,
                Col.fx_rate: pl.Float64,
            }
        )

    @override
    @classmethod
    def _transform(cls, fx_rates: pl.LazyFrame) -> pl.LazyFrame:
        return fx_rates.filter(pl.col(Col.target_currency) == "GBP").select(
            pl.col(RawCol.currency_code).cast(pl.String).alias(Col.currency),
            pl.col(Col.target_currency).cast(pl.String),
            pl.col(RawCol.rate_date).cast(pl.String).alias(Col.fx_rate_date),
            pl.col(RawCol.rate).cast(pl.Float64).alias(Col.fx_rate),
        )
