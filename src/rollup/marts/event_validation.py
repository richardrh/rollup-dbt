from __future__ import annotations

import polars as pl
import pandera.polars as pa

from rollup.columns import Col
from rollup.metrics import METRIC_LONG_SCHEMA


EVENT_VALIDATION_INPUT_SCHEMA = METRIC_LONG_SCHEMA
EVENT_VALIDATION_SCHEMA = pa.DataFrameSchema(
    {
        Col.base_model: pa.Column(pl.String, nullable=True),
        Col.event_id: pa.Column(pl.Int64, nullable=True),
        Col.missing_model_event_day: pa.Column(pl.Boolean, nullable=True),
    },
    strict=False,
)


def event_validation(frame: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    EVENT_VALIDATION_INPUT_SCHEMA.validate(frame)

    validation = frame.select(
        Col.base_model,
        Col.event_id,
        pl.col(Col.year_id).is_null().alias(Col.missing_model_event_day),
    ).unique()
    return validation
