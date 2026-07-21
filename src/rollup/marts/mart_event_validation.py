from __future__ import annotations

from typing import override

import polars as pl

from rollup.columns import Col, FanoutCol
from rollup.model import PolarsModel


class Model(PolarsModel[[pl.LazyFrame, pl.LazyFrame]]):
    @override
    @classmethod
    def schema(cls) -> pl.Schema:
        return pl.Schema(
            {  # type: ignore[arg-type]  # Polars accepts StrEnum keys.
                Col.base_model: pl.String,
                Col.metric: pl.String,
                Col.forecast_date: pl.Date,
                Col.row_count: pl.UInt32,
                Col.missing_model_event_id: pl.UInt32,
                Col.missing_model_event_day: pl.UInt32,
            }
        )

    @override
    @classmethod
    def _transform(
        cls, main_fanout: pl.LazyFrame, dialsup_fanout: pl.LazyFrame
    ) -> pl.LazyFrame:
        reports = [
            fanout.group_by(Col.base_model, Col.metric, Col.forecast_date).agg(
                pl.len().alias(Col.row_count),
                pl.col(FanoutCol.ModelEventID)
                .is_null()
                .sum()
                .alias(Col.missing_model_event_id),
                pl.col(FanoutCol.ModelEventDay)
                .is_null()
                .sum()
                .alias(Col.missing_model_event_day),
            )
            for fanout in [main_fanout, dialsup_fanout]
        ]
        return pl.concat(reports, how="vertical").select(
            Col.base_model,
            Col.metric,
            Col.forecast_date,
            pl.col(Col.row_count).cast(pl.UInt32),
            pl.col(Col.missing_model_event_id).cast(pl.UInt32),
            pl.col(Col.missing_model_event_day).cast(pl.UInt32),
        )
