from __future__ import annotations

import polars as pl

from rollup.columns import Col


def event_validation(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.select(
        Col.base_model,
        Col.event_id,
        pl.col(Col.year_id).is_null().alias(Col.missing_model_event_day),
    ).unique()
