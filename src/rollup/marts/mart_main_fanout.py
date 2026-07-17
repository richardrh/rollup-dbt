from __future__ import annotations
import polars as pl
from rollup.columns import Col
from rollup.marts._fanout_helpers import build_fanout
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
    require_join_key_compatible,
)

MODEL = "mart_main_fanout"


def validate(ylt_thresholded: pl.LazyFrame, risklink_events: pl.LazyFrame) -> None:
    ylt_schema = collect_lazy_schema(MODEL, "ylt_thresholded", ylt_thresholded)
    risklink_schema = collect_lazy_schema(MODEL, "risklink_events", risklink_events)
    require_columns(
        MODEL,
        "ylt_thresholded",
        ylt_schema,
        [
            Col.metric,
            Col.base_model,
            Col.event_id,
            Col.year_id,
            Col.region_peril_id,
            Col.model_event_id,
            Col.event_day,
            Col.forecast_date,
            Col.target_currency,
            Col.cds_cat_class_name,
        ],
    )
    require_join_key_compatible(
        MODEL,
        "ylt_thresholded",
        ylt_schema,
        "risklink_events",
        risklink_schema,
        [Col.event_id, Col.region_peril_id],
    )
    require_dtype_family(MODEL, "ylt_thresholded", ylt_schema, Col.year_id, "numeric")
    require_dtype_family(
        MODEL, "risklink_events", risklink_schema, Col.model_occurrence_year, "numeric"
    )
    require_dtype_family(MODEL, "ylt_thresholded", ylt_schema, Col.loss, "numeric")
    require_columns(
        MODEL,
        "risklink_events",
        risklink_schema,
        [
            Col.event_id,
            Col.model_occurrence_year,
            Col.region_peril_id,
            Col.risklink_event_day,
        ],
    )


def transform(
    ylt_thresholded: pl.LazyFrame, risklink_events: pl.LazyFrame
) -> pl.LazyFrame:
    validate(ylt_thresholded, risklink_events)
    frame = build_fanout(
        ylt_thresholded.filter(pl.col(Col.metric) == "euws_override"), risklink_events
    )
    validate_output(MODEL, frame)
    return frame
