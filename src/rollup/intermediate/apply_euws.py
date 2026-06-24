from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol


def apply_euws(
    frame: pl.LazyFrame,
    verisk_events: pl.LazyFrame,
    euws_factors: pl.DataFrame,
    euws_overrides: pl.DataFrame,
) -> pl.LazyFrame:
    factors = prepare_euws_factors(euws_factors)
    overrides = prepare_euws_overrides(euws_overrides)
    with_raw_factor = (
        frame.join(
            verisk_events,
            on=[Col.event_id, Col.year_id, Col.model_code],
            how="left",
        )
        .join(factors, on=[Col.model_event_id, Col.year_id], how="left")
        .with_columns(
            pl.when(pl.col(Col.is_euws) == 1)
            .then(pl.col(Col.euws_factor_raw_source).fill_null(1.0))
            .otherwise(pl.lit(1.0))
            .alias(Col.euws_factor_raw)
        )
        .with_columns(
            (pl.col("forecast_loss") * pl.col(Col.euws_factor_raw)).alias(
                Col.original_ylt_loss_blended_gbp_forecast_euws_raw
            )
        )
        .drop(Col.euws_factor_raw_source)
    )
    override_condition = (
        pl.col(Col.euws_override_factor).is_not_null()
        & (pl.col(Col.rnk) <= pl.col(Col.euws_override_max_rank))
        & (pl.col(Col.euws_factor_raw) == 0)
    )
    return with_raw_factor.join(overrides, on=Col.rollup_lob, how="left").with_columns(
        override_condition.alias(Col.euws_override_applied),
        pl.when(override_condition)
        .then(pl.col(Col.euws_override_factor))
        .otherwise(pl.col(Col.euws_factor_raw))
        .alias(Col.euws_factor),
    ).with_columns(
        (pl.col("forecast_loss") * pl.col(Col.euws_factor)).alias("euws_loss")
    )


def prepare_euws_factors(euws_factors: pl.DataFrame) -> pl.LazyFrame:
    if euws_factors.is_empty():
        return pl.DataFrame(
            schema={
                Col.model_event_id: pl.Int64,
                Col.year_id: pl.Int64,
                Col.euws_factor_raw_source: pl.Float64,
            }
        ).lazy()
    return euws_factors.lazy().select(
        pl.col(Col.model_event_id).cast(pl.Int64),
        pl.col(RawCol.occ_year).cast(pl.Int64).alias(Col.year_id),
        pl.col(RawCol.factor).cast(pl.Float64).alias(Col.euws_factor_raw_source),
    )


def prepare_euws_overrides(euws_overrides: pl.DataFrame) -> pl.LazyFrame:
    if euws_overrides.is_empty():
        return pl.DataFrame(
            schema={
                Col.rollup_lob: pl.String,
                Col.euws_override_max_rank: pl.Int64,
                Col.euws_override_factor: pl.Float64,
            }
        ).lazy()
    return euws_overrides.lazy().select(
        Col.rollup_lob,
        pl.col(RawCol.max_rank).cast(pl.Int64).alias(Col.euws_override_max_rank),
        pl.col(RawCol.factor).cast(pl.Float64).alias(Col.euws_override_factor),
    )
