from __future__ import annotations

import polars as pl
from rollup.model_validation import validate_schema

from rollup.columns import Col, FanoutCol, RawCol

MODEL = "stg_risklink_flood_events"


def schema() -> pl.Schema:
    return pl.Schema(
        {  # type: ignore[arg-type]  # Polars accepts StrEnum keys.
            Col.event_id: pl.Int64,
            Col.model_occurrence_year: pl.Int64,
            Col.region_peril_id: pl.Int64,
            Col.risklink_event_day: pl.Int64,
        }
    )


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(raw_events: pl.LazyFrame) -> pl.LazyFrame:
    frame = (
        raw_events.group_by(
            FanoutCol.ModelEventID, RawCol.ModelOccurrenceYear, RawCol.RegionPerilID
        )
        .agg(pl.col(RawCol.ModelOccurrenceDate).min().alias(Col.model_occurrence_date))
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
    validate(frame)
    return frame
