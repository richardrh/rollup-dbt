from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.intermediate.build_metric_long import METRIC_LONG_SCHEMA
from rollup.schemas import require_columns


EVENT_VALIDATION_INPUT_SCHEMA = METRIC_LONG_SCHEMA
EVENT_VALIDATION_SCHEMA = pl.Schema(
    {
        Col.base_model: pl.String,
        Col.event_id: pl.Int64,
        Col.missing_model_event_day: pl.Boolean,
    }
)


def event_validation(frame: pl.DataFrame) -> pl.DataFrame:
    require_columns(frame, EVENT_VALIDATION_INPUT_SCHEMA)

    validation = frame.select(
        Col.base_model,
        Col.event_id,
        pl.col(Col.year_id).is_null().alias(Col.missing_model_event_day),
    ).unique()
    require_columns(validation, EVENT_VALIDATION_SCHEMA)
    return validation
