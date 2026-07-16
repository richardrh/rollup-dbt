from __future__ import annotations
import polars as pl
from rollup.columns import Col
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
)

MODEL = "int_ylt_main_metric_stream"


def validate(
    ylt_ranked: pl.LazyFrame,
    ylt_blended: pl.LazyFrame,
    ylt_localccy: pl.LazyFrame,
    ylt_localccy_forecast: pl.LazyFrame,
    ylt_euws: pl.LazyFrame,
    ylt_euws_override: pl.LazyFrame,
) -> None:
    for input_name, frame in {
        "ylt_ranked": ylt_ranked,
        "ylt_blended": ylt_blended,
        "ylt_localccy": ylt_localccy,
        "ylt_localccy_forecast": ylt_localccy_forecast,
        "ylt_euws": ylt_euws,
        "ylt_euws_override": ylt_euws_override,
    }.items():
        schema = collect_lazy_schema(MODEL, input_name, frame)
        require_columns(MODEL, input_name, schema, [Col.vendor, Col.metric, Col.loss])
        require_dtype_family(MODEL, input_name, schema, Col.loss, "numeric")


def transform(
    ylt_ranked: pl.LazyFrame,
    ylt_blended: pl.LazyFrame,
    ylt_localccy: pl.LazyFrame,
    ylt_localccy_forecast: pl.LazyFrame,
    ylt_euws: pl.LazyFrame,
    ylt_euws_override: pl.LazyFrame,
) -> pl.LazyFrame:
    validate(
        ylt_ranked,
        ylt_blended,
        ylt_localccy,
        ylt_localccy_forecast,
        ylt_euws,
        ylt_euws_override,
    )
    frame = pl.concat(
        [
            ylt_ranked,
            ylt_blended,
            ylt_localccy,
            ylt_localccy_forecast,
            ylt_euws,
            ylt_euws_override,
        ],
        how="diagonal",
    )
    validate_output(MODEL, frame)
    return frame
