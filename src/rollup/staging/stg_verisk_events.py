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
                Col.model_event_id: pl.Int64,
                Col.model_code: pl.Int64,
                Col.event_id: pl.Int64,
                Col.year_id: pl.Int64,
                Col.event_day: pl.Int64,
            }
        )

    @override
    @classmethod
    def _transform(cls, raw_events: pl.LazyFrame) -> pl.LazyFrame:
        return raw_events.select(
            pl.col(RawCol.EventID).cast(pl.Int64).alias(Col.model_event_id),
            pl.col(RawCol.ModelID).cast(pl.Int64).alias(Col.model_code),
            pl.col(RawCol.Event).cast(pl.Int64).alias(Col.event_id),
            pl.col(RawCol.Year).cast(pl.Int64).alias(Col.year_id),
            pl.col(RawCol.Day).cast(pl.Int64).alias(Col.event_day),
        )
