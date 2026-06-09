from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.intermediate.build_metric_long import METRIC_LONG_SCHEMA


EVENT_VALIDATION_INPUT_SCHEMA = METRIC_LONG_SCHEMA
EVENT_VALIDATION_SCHEMA = pl.Schema(
    {
        Col.base_model: pl.String,
        Col.event_id: pl.Int64,
        Col.missing_model_event_day: pl.Boolean,
    }
)


def event_validation(frame: pl.DataFrame) -> pl.DataFrame:
    actual = frame.schema
    missing = [str(name) for name in EVENT_VALIDATION_INPUT_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"event_validation missing columns: {missing}")

    validation = frame.select(
        Col.base_model,
        Col.event_id,
        pl.col(Col.year_id).is_null().alias(Col.missing_model_event_day),
    ).unique()
    actual = validation.schema
    missing = [str(name) for name in EVENT_VALIDATION_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"event_validation missing columns: {missing}")
    return validation
