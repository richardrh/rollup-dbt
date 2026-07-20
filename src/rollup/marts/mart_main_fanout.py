from __future__ import annotations
import polars as pl
from rollup.model_validation import validate_schema
from rollup.columns import Col
from rollup.marts._fanout_helpers import build_fanout

MODEL = "mart_main_fanout"


def schema() -> pl.Schema:
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


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(
    ylt_thresholded: pl.LazyFrame, risklink_events: pl.LazyFrame
) -> pl.LazyFrame:
    frame = build_fanout(
        ylt_thresholded.filter(pl.col(Col.metric) == "euws_override"), risklink_events
    ).select(
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
    validate(frame)
    return frame
