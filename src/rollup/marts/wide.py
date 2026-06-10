from __future__ import annotations

import polars as pl
import pandera.polars as pa

from rollup.columns import Col
from rollup.metrics import METRIC_LONG_SCHEMA


WIDE_INPUT_SCHEMA = METRIC_LONG_SCHEMA
WIDE_OUTPUT_SCHEMA = pa.DataFrameSchema(
    {
        Col.vendor: pa.Column(pl.String, nullable=True),
        Col.base_model: pa.Column(pl.String, nullable=True),
        Col.analysis_id: pa.Column(pl.String, nullable=True),
        Col.modelled_lob: pa.Column(pl.String, nullable=True),
        Col.modelled_peril: pa.Column(pl.String, nullable=True),
        Col.rollup_lob: pa.Column(pl.String, nullable=True),
        Col.rollup_peril: pa.Column(pl.String, nullable=True),
        Col.region_peril_id: pa.Column(pl.Int64, nullable=True),
        Col.class_: pa.Column(pl.String, nullable=True),
        Col.office: pa.Column(pl.String, nullable=True),
        Col.currency: pa.Column(pl.String, nullable=True),
        Col.target_currency: pa.Column(pl.String, nullable=True),
        Col.year_id: pa.Column(pl.Int64, nullable=True),
        Col.event_id: pa.Column(pl.Int64, nullable=True),
        Col.is_dialsup: pa.Column(pl.Int64, nullable=True),
    },
    strict=False,
)

_WIDE_PIVOT_COLUMNS = {Col.metric, Col.forecast_date, Col.loss}


def wide_column_name(metric: str, forecast_date: str) -> str:
    return f"{metric}_{forecast_date.replace('-', '')}"


def wide(frame: pl.DataFrame | pl.LazyFrame, target_currency: str = "GBP") -> pl.DataFrame | pl.LazyFrame:
    del target_currency
    WIDE_INPUT_SCHEMA.validate(frame)
    source = frame.lazy() if isinstance(frame, pl.DataFrame) else frame
    dimensions = [
        column
        for column in source.collect_schema().names()
        if column not in _WIDE_PIVOT_COLUMNS and not column.startswith("_")
    ]
    metric_dates = (
        source.select(Col.metric, Col.forecast_date)
        .unique()
        .sort(Col.metric, Col.forecast_date)
        .collect()
        .rows(named=True)
    )
    value_columns = []
    for row in metric_dates:
        metric = row[Col.metric]
        forecast_date = row[Col.forecast_date]
        condition = (pl.col(Col.metric) == metric) & (pl.col(Col.forecast_date) == forecast_date)
        value_columns.append(
            pl.when(condition.any())
            .then(pl.col(Col.loss).filter(condition).sum())
            .otherwise(None)
            .alias(wide_column_name(metric, forecast_date))
        )

    wide_frame = source.group_by(dimensions).agg(value_columns)
    if isinstance(frame, pl.DataFrame):
        return wide_frame.collect()
    return wide_frame
