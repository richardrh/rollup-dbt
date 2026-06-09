from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol
from rollup.intermediate.apply_forecast import FORECAST_APPLIED_YLT_SCHEMA


EUWS_INPUT_SCHEMA = FORECAST_APPLIED_YLT_SCHEMA
EUWS_FACTORS_SCHEMA = pl.Schema(
    {
        Col.event_id: pl.Int64,
        RawCol.factor: pl.Float64,
    }
)
MODEL_EVENT_EUWS_FACTORS_SCHEMA = pl.Schema(
    {
        Col.model_event_id: pl.Int64,
        RawCol.factor: pl.Float64,
    }
)
EUWS_APPLIED_YLT_SCHEMA = pl.Schema(
    {
        **EUWS_INPUT_SCHEMA,
        Col.euws_factor: pl.Float64,
        "euws_loss": pl.Float64,
    }
)


def apply_euws(frame: pl.LazyFrame, euws_factors: pl.DataFrame) -> pl.LazyFrame:
    actual = frame.collect_schema()
    missing = [str(name) for name in EUWS_INPUT_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"apply_euws missing columns: {missing}")

    if euws_factors.is_empty():
        applied = frame.with_columns(
            pl.lit(1.0).alias(Col.euws_factor),
            pl.col("forecast_loss").alias("euws_loss"),
        )
        actual = applied.collect_schema()
        missing = [str(name) for name in EUWS_APPLIED_YLT_SCHEMA if name not in actual]
        if missing:
            raise ValueError(f"apply_euws missing columns: {missing}")
        return applied
    event_col = Col.model_event_id if Col.model_event_id in euws_factors.columns else Col.event_id
    factor_schema = (
        MODEL_EVENT_EUWS_FACTORS_SCHEMA if Col.model_event_id in euws_factors.columns else EUWS_FACTORS_SCHEMA
    )
    actual = euws_factors.schema
    missing = [str(name) for name in factor_schema if name not in actual]
    if missing:
        raise ValueError(f"apply_euws missing columns: {missing}")
    factors = euws_factors.lazy().select(
        pl.col(event_col).cast(pl.Int64).alias(Col.event_id),
        pl.col(RawCol.factor).cast(pl.Float64).alias(Col.euws_factor),
    ).unique(Col.event_id, keep="last")
    applied = frame.join(factors, on=Col.event_id, how="left").with_columns(
        pl.col(Col.euws_factor).fill_null(1.0)
    ).with_columns((pl.col("forecast_loss") * pl.col(Col.euws_factor)).alias("euws_loss"))
    actual = applied.collect_schema()
    missing = [str(name) for name in EUWS_APPLIED_YLT_SCHEMA if name not in actual]
    if missing:
        raise ValueError(f"apply_euws missing columns: {missing}")
    return applied
