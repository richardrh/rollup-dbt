from __future__ import annotations

import polars as pl
from rollup.model_validation import validate_schema

from rollup.columns import Col, RawCol

MODEL = "stg_verisk_events"


def schema() -> pl.Schema:
    return pl.Schema(
        {  # type: ignore[arg-type]  # Polars accepts StrEnum keys.
            Col.model_event_id: pl.Int64,
            Col.model_code: pl.Int64,
            Col.event_id: pl.Int64,
            Col.year_id: pl.Int64,
            Col.event_day: pl.Int64,
        }
    )


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(raw_events: pl.LazyFrame) -> pl.LazyFrame:
    frame = raw_events.select(
        pl.col(RawCol.EventID).cast(pl.Int64).alias(Col.model_event_id),
        pl.col(RawCol.ModelID).cast(pl.Int64).alias(Col.model_code),
        pl.col(RawCol.Event).cast(pl.Int64).alias(Col.event_id),
        pl.col(RawCol.Year).cast(pl.Int64).alias(Col.year_id),
        pl.col(RawCol.Day).cast(pl.Int64).alias(Col.event_day),
    )
    validate(frame)
    return frame
