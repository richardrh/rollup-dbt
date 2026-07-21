from __future__ import annotations

from typing import override

import polars as pl

from rollup.columns import Col
from rollup.model import PolarsModel


class Model(PolarsModel[[pl.LazyFrame]]):
    @override
    @classmethod
    def schema(cls) -> pl.Schema:
        return pl.Schema(
            {Col.forecast_date: pl.Date}  # type: ignore[arg-type]  # Polars accepts StrEnum keys.
        )

    @override
    @classmethod
    def _transform(cls, forecast_factors: pl.LazyFrame) -> pl.LazyFrame:
        return (
            forecast_factors.select(pl.col(Col.forecast_date).cast(pl.Date))
            .unique()
            .select(pl.col(Col.forecast_date).cast(pl.Date))
        )
