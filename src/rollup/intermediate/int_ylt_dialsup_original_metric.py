from __future__ import annotations
import polars as pl
from rollup.columns import Col
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
)

MODEL = "int_ylt_dialsup_original_metric"


def validate(ylt_with_factors: pl.LazyFrame) -> None:
    require_columns(
        MODEL,
        "ylt_with_factors",
        collect_lazy_schema(MODEL, "ylt_with_factors", ylt_with_factors),
        [Col.loss],
    )


def transform(ylt_with_factors: pl.LazyFrame) -> pl.LazyFrame:
    validate(ylt_with_factors)
    frame = ylt_with_factors.with_columns(
        pl.lit("dialsup_original").alias(Col.metric)
    ).drop("_forecast_factor_raw", "_forecast_factor")
    validate_output(MODEL, frame)
    return frame
