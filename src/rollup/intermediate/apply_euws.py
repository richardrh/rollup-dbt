from __future__ import annotations

import polars as pl
import pandera.polars as pa

from rollup.columns import Col, RawCol
from rollup.intermediate.apply_forecast import FORECAST_APPLIED_YLT_SCHEMA


EUWS_INPUT_SCHEMA = FORECAST_APPLIED_YLT_SCHEMA
EUWS_FACTORS_SCHEMA = pa.DataFrameSchema(
    {
        Col.event_id: pa.Column(pl.Int64, nullable=True),
        RawCol.factor: pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)
MODEL_EVENT_EUWS_FACTORS_SCHEMA = pa.DataFrameSchema(
    {
        Col.model_event_id: pa.Column(pl.Int64, nullable=True),
        RawCol.factor: pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)
EUWS_APPLIED_YLT_SCHEMA = pa.DataFrameSchema(
    {
        **EUWS_INPUT_SCHEMA.columns,
        Col.euws_factor: pa.Column(pl.Float64, nullable=True),
        "euws_loss": pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)


def apply_euws(frame: pl.LazyFrame, euws_factors: pl.DataFrame) -> pl.LazyFrame:
    EUWS_INPUT_SCHEMA.validate(frame)

    if euws_factors.is_empty():
        return frame.with_columns(
            pl.lit(1.0).alias(Col.euws_factor),
            pl.col("forecast_loss").alias("euws_loss"),
        )
    event_col = Col.model_event_id if Col.model_event_id in euws_factors.columns else Col.event_id
    factor_schema = (
        MODEL_EVENT_EUWS_FACTORS_SCHEMA if Col.model_event_id in euws_factors.columns else EUWS_FACTORS_SCHEMA
    )
    factor_schema.validate(euws_factors)
    factors = euws_factors.lazy().select(
        pl.col(event_col).cast(pl.Int64).alias(Col.event_id),
        pl.col(RawCol.factor).cast(pl.Float64).alias(Col.euws_factor),
    ).unique(Col.event_id, keep="last")
    applied = frame.join(factors, on=Col.event_id, how="left").with_columns(
        pl.col(Col.euws_factor).fill_null(1.0)
    ).with_columns((pl.col("forecast_loss") * pl.col(Col.euws_factor)).alias("euws_loss"))
    return applied
