from __future__ import annotations
import polars as pl
from rollup.columns import Col
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
)

MODEL = "mart_ylt_main_long"


def validate(ylt: pl.LazyFrame, threshold: float) -> None:
    schema = collect_lazy_schema(MODEL, "ylt", ylt)
    require_columns(MODEL, "ylt", schema, [Col.metric, Col.base_model])
    require_dtype_family(MODEL, "ylt", schema, Col.loss, "numeric")


def transform(ylt: pl.LazyFrame, threshold: float) -> pl.LazyFrame:
    validate(ylt, threshold)
    threshold_predicate = (
        pl.col(Col.loss).is_not_null()
        if threshold <= 0
        else pl.col(Col.loss) >= threshold
    )
    frame = ylt.filter(
        (pl.col(Col.metric) != "euws_override") | threshold_predicate
    ).with_columns(
        pl.when(pl.col(Col.metric) == "euws_override")
        .then(pl.lit("cds_main"))
        .otherwise(pl.lit("intermediate_audit"))
        .alias(Col.output_use)
    )
    validate_output(MODEL, frame)
    return frame
