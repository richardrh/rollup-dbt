from __future__ import annotations

from typing import override

import polars as pl

from rollup.columns import Col, FanoutCol, RawCol
from rollup.model import PolarsModel


class Model(PolarsModel[[pl.LazyFrame]]):
    @override
    @classmethod
    def schema(cls) -> pl.Schema:
        return pl.Schema(
            {  # type: ignore[arg-type]  # Polars accepts StrEnum keys.
                Col.event_id: pl.Int64,
                Col.model_occurrence_year: pl.Int64,
                Col.region_peril_id: pl.Int64,
                Col.risklink_event_day: pl.Int64,
            }
        )

    @override
    @classmethod
    def _transform(cls, raw_events: pl.LazyFrame) -> pl.LazyFrame:
        return (
            raw_events.group_by(
                FanoutCol.ModelEventID, RawCol.ModelOccurrenceYear, RawCol.RegionPerilID
            )
            .agg(
                pl.col(RawCol.ModelOccurrenceDate)
                .min()
                .alias(Col.model_occurrence_date)
            )
            .select(
                pl.col(FanoutCol.ModelEventID).cast(pl.Int64).alias(Col.event_id),
                pl.col(RawCol.ModelOccurrenceYear)
                .cast(pl.Int64)
                .alias(Col.model_occurrence_year),
                pl.col(RawCol.RegionPerilID).cast(pl.Int64).alias(Col.region_peril_id),
                pl.col(Col.model_occurrence_date)
                .dt.ordinal_day()
                .cast(pl.Int64)
                .alias(Col.risklink_event_day),
            )
        )
