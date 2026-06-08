from __future__ import annotations

import polars as pl

from rollup.columns import Col


def wide(frame: pl.DataFrame) -> pl.DataFrame:
    index = [
        Col.base_model,
        Col.analysis_id,
        Col.rollup_lob,
        Col.rollup_peril,
        Col.year_id,
        Col.event_id,
        Col.forecast_date,
    ]
    return frame.pivot(index=index, on=Col.metric, values=Col.loss, aggregate_function="sum")
