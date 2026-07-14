from __future__ import annotations
# mypy: ignore-errors

import polars as pl

from rollup.columns import Col


def enrich_dialsup_ylt_with_factors(
    ylt: pl.LazyFrame,
    verisk_events: pl.LazyFrame,
    fx_rates: pl.LazyFrame,
    forecast_dates: pl.LazyFrame,
    forecast_factors: pl.LazyFrame,
) -> pl.LazyFrame:
    return (
        ylt.join(
            verisk_events,
            on=[Col.event_id, Col.year_id, Col.model_code],
            how="left",
        )
        .join(fx_rates, on=Col.currency, how="inner")
        .join(forecast_dates, how="cross")
        .join(
            forecast_factors,
            on=[Col.class_, Col.office, Col.forecast_date],
            how="left",
        )
        .with_columns(
            pl.col("_forecast_factor_raw").fill_null(1.0).alias("_forecast_factor"),
        )
    )


def convert_dialsup_to_local_currency(ylt_with_factors: pl.LazyFrame) -> pl.LazyFrame:
    return ylt_with_factors.with_columns(
        (pl.col(Col.loss) / pl.col(Col.fx_rate)).alias(Col.loss),
        pl.col(Col.currency).alias(Col.target_currency),
        pl.lit("dialsup_localccy").alias(Col.metric),
    )


def apply_forecast_factors_to_dialsup_ylt(ylt_localccy: pl.LazyFrame) -> pl.LazyFrame:
    return ylt_localccy.with_columns(
        (pl.col(Col.loss) * pl.col("_forecast_factor")).alias(Col.loss),
        pl.lit("dialsup_localccy_forecast").alias(Col.metric),
    )


def drop_dialsup_factor_columns(frame: pl.LazyFrame) -> pl.LazyFrame:
    return frame.drop("_forecast_factor_raw", "_forecast_factor")
