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

WIDE_METRICS = ("euws_override", "dialsup_gbp_forecast")


def wide(frame: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
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
    source = frame.lazy() if isinstance(frame, pl.DataFrame) else frame
    wide_frame = source.group_by(index).agg(
        [
            pl.when((pl.col(Col.metric) == metric).any())
            .then(pl.col(Col.loss).filter(pl.col(Col.metric) == metric).sum())
            .otherwise(None)
            .alias(metric)
            for metric in WIDE_METRICS
        ]
    )
    if isinstance(frame, pl.DataFrame):
        return wide_frame.collect()
    return wide_frame
