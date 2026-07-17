from __future__ import annotations
import polars as pl
from rollup.columns import Col, FanoutCol
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
)

MODEL = "mart_event_validation"


def validate(main_fanout: pl.LazyFrame, dialsup_fanout: pl.LazyFrame) -> None:
    for input_name, frame in {
        "main_fanout": main_fanout,
        "dialsup_fanout": dialsup_fanout,
    }.items():
        schema = collect_lazy_schema(MODEL, input_name, frame)
        require_columns(
            MODEL,
            input_name,
            schema,
            [
                Col.base_model,
                Col.metric,
                Col.forecast_date,
                FanoutCol.ModelEventID,
                FanoutCol.ModelEventDay,
            ],
        )


def transform(main_fanout: pl.LazyFrame, dialsup_fanout: pl.LazyFrame) -> pl.LazyFrame:
    validate(main_fanout, dialsup_fanout)
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
    frame = pl.concat(reports, how="vertical")
    validate_output(MODEL, frame)
    return frame
