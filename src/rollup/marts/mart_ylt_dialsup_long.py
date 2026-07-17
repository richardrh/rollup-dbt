from __future__ import annotations
import polars as pl
from rollup.columns import Col
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
)

MODEL = "mart_ylt_dialsup_long"


def validate(ylt_dialsup: pl.LazyFrame, threshold: float) -> None:
    schema = collect_lazy_schema(MODEL, "ylt_dialsup", ylt_dialsup)
    require_columns(MODEL, "ylt_dialsup", schema, [Col.metric])
    require_dtype_family(MODEL, "ylt_dialsup", schema, Col.loss, "numeric")


def transform(ylt_dialsup: pl.LazyFrame, threshold: float) -> pl.LazyFrame:
    validate(ylt_dialsup, threshold)
    threshold_predicate = (
        pl.col(Col.loss).is_not_null()
        if threshold <= 0
        else pl.col(Col.loss) >= threshold
    )
    frame = ylt_dialsup.filter(
        (pl.col(Col.metric) == "dialsup_localccy_forecast") & threshold_predicate
    ).with_columns(pl.lit("cds_dialsup").alias(Col.output_use))
    validate_output(MODEL, frame)
    return frame
