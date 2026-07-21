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
            {
                Col.class_: pl.String,
                Col.office: pl.String,
                Col.forecast_date: pl.Date,
                "_forecast_factor_raw": pl.Float64,
            }
        )

    @override
    @classmethod
    def _transform(cls, forecast_factors: pl.LazyFrame) -> pl.LazyFrame:
        return forecast_factors.select(
            Col.class_,
            pl.col("office_iso2").alias(Col.office),
            pl.col(Col.forecast_date)
            .cast(pl.String)
            .str.to_date("%Y-%m-%d", strict=True)
            .alias(Col.forecast_date),
            pl.col(RawCol.factor).alias("_forecast_factor_raw"),
        )
