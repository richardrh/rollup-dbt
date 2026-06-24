from __future__ import annotations

import polars as pl

from rollup.columns import Col

_WIDE_PIVOT_COLUMNS = {Col.metric, Col.forecast_date, Col.loss}


def wide_column_name(metric: str, forecast_date: str) -> str:
    return f"{metric}_{forecast_date.replace('-', '')}"


def wide(frame: pl.DataFrame | pl.LazyFrame, target_currency: str = "GBP") -> pl.DataFrame | pl.LazyFrame:
    del target_currency
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
