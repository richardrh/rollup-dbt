from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol


def apply_euws(frame: pl.LazyFrame, euws_factors: pl.DataFrame) -> pl.LazyFrame:
    if euws_factors.is_empty():
        return frame.with_columns(pl.lit(1.0).alias(Col.euws_factor), pl.col("forecast_loss").alias("euws_loss"))
    event_col = Col.model_event_id if Col.model_event_id in euws_factors.columns else Col.event_id
    factors = euws_factors.lazy().select(
        pl.col(event_col).cast(pl.Int64).alias(Col.event_id),
        pl.col(RawCol.factor).cast(pl.Float64).alias(Col.euws_factor),
    ).unique(Col.event_id, keep="last")
    return frame.join(factors, on=Col.event_id, how="left").with_columns(
        pl.col(Col.euws_factor).fill_null(1.0)
    ).with_columns((pl.col("forecast_loss") * pl.col(Col.euws_factor)).alias("euws_loss"))
