from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
)

MODEL = "stg_verisk_events"


def validate(raw_events: pl.LazyFrame) -> None:
    schema = collect_lazy_schema(MODEL, "raw_events", raw_events)
    for column in [RawCol.EventID, RawCol.Event, RawCol.Year, RawCol.Day]:
        require_dtype_family(MODEL, "raw_events", schema, column, "integer")
    require_columns(MODEL, "raw_events", schema, [RawCol.ModelID])


def transform(raw_events: pl.LazyFrame) -> pl.LazyFrame:
    validate(raw_events)
    frame = raw_events.select(
        pl.col(RawCol.EventID).alias(Col.model_event_id),
        pl.col(RawCol.ModelID).alias(Col.model_code),
        pl.col(RawCol.Event).alias(Col.event_id),
        pl.col(RawCol.Year).alias(Col.year_id),
        pl.col(RawCol.Day).alias(Col.event_day),
    )
    validate_output(MODEL, frame)
    return frame
