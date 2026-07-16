from __future__ import annotations
import polars as pl
from rollup.columns import Col
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
)

MODEL = "int_ylt_dialsup_forecast_metric"


def validate(ylt_with_factors: pl.LazyFrame) -> None:
    schema = collect_lazy_schema(MODEL, "ylt_with_factors", ylt_with_factors)
    require_columns(
        MODEL,
        "ylt_with_factors",
        schema,
        [Col.loss, Col.fx_rate, Col.currency, "_forecast_factor"],
    )
    require_dtype_family(
        MODEL, "ylt_with_factors", schema, "_forecast_factor", "numeric"
    )


def transform(ylt_with_factors: pl.LazyFrame) -> pl.LazyFrame:
    validate(ylt_with_factors)
    local = ylt_with_factors.with_columns(
        (pl.col(Col.loss) / pl.col(Col.fx_rate)).alias(Col.loss),
        pl.col(Col.currency).alias(Col.target_currency),
        pl.lit("dialsup_localccy").alias(Col.metric),
    )
    frame = local.with_columns(
        (pl.col(Col.loss) * pl.col("_forecast_factor")).alias(Col.loss),
        pl.lit("dialsup_localccy_forecast").alias(Col.metric),
    ).drop("_forecast_factor_raw", "_forecast_factor")
    validate_output(MODEL, frame)
    return frame
