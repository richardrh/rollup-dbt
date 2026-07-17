from __future__ import annotations
import polars as pl
from rollup.columns import Col
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
)

MODEL = "int_ylt_dialsup_metric_stream"


def validate(
    dialsup_original: pl.LazyFrame,
    dialsup_localccy: pl.LazyFrame,
    dialsup_localccy_forecast: pl.LazyFrame,
) -> None:
    for input_name, frame in {
        "dialsup_original": dialsup_original,
        "dialsup_localccy": dialsup_localccy,
        "dialsup_localccy_forecast": dialsup_localccy_forecast,
    }.items():
        schema = collect_lazy_schema(MODEL, input_name, frame)
        require_columns(MODEL, input_name, schema, [Col.vendor, Col.metric, Col.loss])
        require_dtype_family(MODEL, input_name, schema, Col.loss, "numeric")


def transform(
    dialsup_original: pl.LazyFrame,
    dialsup_localccy: pl.LazyFrame,
    dialsup_localccy_forecast: pl.LazyFrame,
) -> pl.LazyFrame:
    validate(dialsup_original, dialsup_localccy, dialsup_localccy_forecast)
    frame = pl.concat([dialsup_original, dialsup_localccy, dialsup_localccy_forecast])
    validate_output(MODEL, frame)
    return frame
