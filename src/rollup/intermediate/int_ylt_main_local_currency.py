from __future__ import annotations
import polars as pl
from rollup.columns import Col
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
    require_join_key_compatible,
)

MODEL = "int_ylt_main_local_currency"


def validate(ylt_blended: pl.LazyFrame, fx_rates: pl.LazyFrame) -> None:
    left = collect_lazy_schema(MODEL, "ylt_blended", ylt_blended)
    right = collect_lazy_schema(MODEL, "fx_rates", fx_rates)
    require_columns(MODEL, "ylt_blended", left, [Col.currency, Col.loss])
    require_columns(
        MODEL, "fx_rates", right, [Col.currency, Col.fx_rate, Col.fx_rate_date]
    )
    require_join_key_compatible(
        MODEL, "ylt_blended", left, "fx_rates", right, [Col.currency]
    )
    require_dtype_family(MODEL, "ylt_blended", left, Col.loss, "numeric")
    require_dtype_family(MODEL, "fx_rates", right, Col.fx_rate, "numeric")


def transform(ylt_blended: pl.LazyFrame, fx_rates: pl.LazyFrame) -> pl.LazyFrame:
    validate(ylt_blended, fx_rates)
    frame = (
        ylt_blended.join(fx_rates, on=Col.currency, how="inner")
        .with_columns(
            (pl.col(Col.loss) / pl.col(Col.fx_rate)).alias(Col.loss),
            pl.col(Col.currency).alias(Col.target_currency),
            pl.lit("localccy").alias(Col.metric),
        )
        .drop(Col.fx_rate_date, Col.fx_rate)
    )
    validate_output(MODEL, frame)
    return frame
