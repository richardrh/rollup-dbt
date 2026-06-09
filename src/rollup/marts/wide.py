from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.intermediate.build_metric_long import METRIC_LONG_SCHEMA


WIDE_INPUT_SCHEMA = METRIC_LONG_SCHEMA
WIDE_OUTPUT_SCHEMA = pl.Schema(
    {
        Col.base_model: pl.String,
        Col.analysis_id: pl.String,
        Col.rollup_lob: pl.String,
        Col.rollup_peril: pl.String,
        Col.year_id: pl.Int64,
        Col.event_id: pl.Int64,
        Col.forecast_date: pl.String,
    }
)


def wide(frame: pl.DataFrame) -> pl.DataFrame:
    actual = frame.schema
    missing = [str(name) for name in WIDE_INPUT_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"wide missing columns: {missing}")

    index = [
        Col.base_model,
        Col.analysis_id,
        Col.rollup_lob,
        Col.rollup_peril,
        Col.year_id,
        Col.event_id,
        Col.forecast_date,
    ]
    wide_frame = frame.pivot(index=index, on=Col.metric, values=Col.loss, aggregate_function="sum")
    actual = wide_frame.schema
    missing = [str(name) for name in WIDE_OUTPUT_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"wide missing columns: {missing}")
    return wide_frame
