from __future__ import annotations
import polars as pl
from rollup.model_validation import validate_schema
from rollup.columns import Col, FanoutCol

MODEL = "mart_event_validation"


def schema() -> pl.Schema:
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


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(main_fanout: pl.LazyFrame, dialsup_fanout: pl.LazyFrame) -> pl.LazyFrame:
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
    frame = pl.concat(reports, how="vertical").select(
        Col.base_model,
        Col.metric,
        Col.forecast_date,
        pl.col(Col.row_count).cast(pl.UInt32),
        pl.col(Col.missing_model_event_id).cast(pl.UInt32),
        pl.col(Col.missing_model_event_day).cast(pl.UInt32),
    )
    validate(frame)
    return frame
