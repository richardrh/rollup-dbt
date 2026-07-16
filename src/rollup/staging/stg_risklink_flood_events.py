from __future__ import annotations

import polars as pl

from rollup.columns import Col, FanoutCol, RawCol
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_dtype_family,
)

MODEL = "stg_risklink_flood_events"


def validate(raw_events: pl.LazyFrame) -> None:
    schema = collect_lazy_schema(MODEL, "raw_events", raw_events)
    for column in [
        FanoutCol.ModelEventID,
        RawCol.ModelOccurrenceYear,
        RawCol.RegionPerilID,
    ]:
        require_dtype_family(MODEL, "raw_events", schema, column, "integer")
    require_dtype_family(
        MODEL, "raw_events", schema, RawCol.ModelOccurrenceDate, "date_like"
    )


def transform(raw_events: pl.LazyFrame) -> pl.LazyFrame:
    validate(raw_events)
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
    validate_output(MODEL, frame)
    return frame
