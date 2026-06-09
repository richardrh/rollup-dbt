from __future__ import annotations

import polars as pl
import pandera.polars as pa

from rollup.columns import Col
from rollup.intermediate.build_metric_long import METRIC_LONG_SCHEMA


WIDE_INPUT_SCHEMA = METRIC_LONG_SCHEMA
WIDE_OUTPUT_SCHEMA = pa.DataFrameSchema(
    {
        Col.base_model: pa.Column(pl.String, nullable=True),
        Col.analysis_id: pa.Column(pl.String, nullable=True),
        Col.rollup_lob: pa.Column(pl.String, nullable=True),
        Col.rollup_peril: pa.Column(pl.String, nullable=True),
        Col.year_id: pa.Column(pl.Int64, nullable=True),
        Col.event_id: pa.Column(pl.Int64, nullable=True),
        Col.forecast_date: pa.Column(pl.String, nullable=True),
    },
    strict=False,
)


def wide(frame: pl.DataFrame) -> pl.DataFrame:
    WIDE_INPUT_SCHEMA.validate(frame)

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
    return wide_frame
