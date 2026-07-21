from __future__ import annotations

from typing import override

import polars as pl

from rollup.columns import Col
from rollup.marts._fanout_helpers import build_fanout
from rollup.model import PolarsModel


class Model(PolarsModel[[pl.LazyFrame, pl.LazyFrame]]):
    @override
    @classmethod
    def schema(cls) -> pl.Schema:
        return pl.Schema(
            {
                Col.forecast_date: pl.Date,
                Col.base_model: pl.String,
                Col.metric: pl.String,
                "ModelEventID": pl.Int64,
                "ModelYear": pl.Int64,
                "CurrencyCode": pl.String,
                "ModelYOA": pl.Int64,
                "ModelGrossLoss": pl.Float64,
                "ModelInwardsReinstatement": pl.Int64,
                "ModelEventDay": pl.Int64,
                "LossClassName": pl.String,
            }
        )

    @override
    @classmethod
    def _transform(
        cls, ylt_dialsup_thresholded: pl.LazyFrame, risklink_events: pl.LazyFrame
    ) -> pl.LazyFrame:
        return build_fanout(ylt_dialsup_thresholded, risklink_events).select(
            Col.forecast_date,
            Col.base_model,
            Col.metric,
            "ModelEventID",
            "ModelYear",
            "CurrencyCode",
            "ModelYOA",
            "ModelGrossLoss",
            "ModelInwardsReinstatement",
            "ModelEventDay",
            "LossClassName",
        )
