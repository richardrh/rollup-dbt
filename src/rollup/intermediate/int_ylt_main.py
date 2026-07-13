from __future__ import annotations
# mypy: ignore-errors

import polars as pl

from rollup.columns import Col, RawCol
from rollup.config import RollupConfig


def enrich_ylt_with_ep_summaries(normalized_ylt: pl.LazyFrame, ep_summary: pl.LazyFrame) -> pl.LazyFrame:
    verisk_keys = (
        ep_summary.filter(pl.col(Col.vendor) == "verisk")
        .select(
            Col.vendor,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.rollup_lob,
            Col.rollup_peril,
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.base_model,
            Col.selection_priority,
            Col.is_dialsup,
            Col.is_euws,
            Col.cds_cat_class_name,
            Col.class_,
            Col.office,
            Col.currency,
        )
        .unique()
    )
    verisk = normalized_ylt.filter(pl.col(Col.vendor) == "verisk").join(
        verisk_keys,
        on=[Col.vendor, Col.modelled_lob, Col.modelled_peril],
        how="inner",
    ).select(
        Col.vendor,
        Col.analysis_id,
        Col.modelled_lob,
        Col.modelled_peril,
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.blend_subregion_peril_id,
        Col.base_model,
        Col.selection_priority,
        Col.is_dialsup,
        Col.is_euws,
        Col.cds_cat_class_name,
        Col.class_,
        Col.office,
        Col.currency,
        Col.model_code,
        Col.year_id,
        Col.event_id,
        Col.loss,
    )

    risklink_lookup = (
        ep_summary.filter(pl.col(Col.vendor) == "risklink")
        .select(
            Col.vendor,
            Col.analysis_id,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.rollup_lob,
            Col.rollup_peril,
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.base_model,
            Col.selection_priority,
            Col.is_dialsup,
            Col.is_euws,
            Col.cds_cat_class_name,
            Col.class_,
            Col.office,
            Col.currency,
        )
        .unique()
    )
    risklink = (
        normalized_ylt.filter(pl.col(Col.vendor) == "risklink").drop(Col.modelled_lob, Col.modelled_peril)
        .join(risklink_lookup, on=[Col.vendor, Col.analysis_id], how="inner")
        .select(
            Col.vendor,
            Col.analysis_id,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.rollup_lob,
            Col.rollup_peril,
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.base_model,
            Col.selection_priority,
            Col.is_dialsup,
            Col.is_euws,
            Col.cds_cat_class_name,
            Col.class_,
            Col.office,
            Col.currency,
            Col.model_code,
            Col.year_id,
            Col.event_id,
            Col.loss,
        )
    )

    return pl.concat([verisk, risklink], how="vertical")


def rank_ylt(ylt: pl.LazyFrame, config: RollupConfig | None = None) -> pl.LazyFrame:
    config = config or RollupConfig()
    partition_keys = [Col.vendor, Col.modelled_lob, Col.rollup_peril]
    schema_names = ylt.collect_schema().names()
    tie_break_keys = [
        column
        for column in [Col.year_id, Col.event_id, Col.analysis_id, Col.model_code]
        if column in schema_names
    ]
    ylt = ylt.sort(
        [*partition_keys, Col.loss, *tie_break_keys],
        descending=[False, False, False, True, *(False for _ in tie_break_keys)],
    )
    vendor_year_expr = pl.lit(None, dtype=pl.Float64)
    for vendor, years in config.blending.vendor_years.items():
        vendor_year_expr = pl.when(pl.col(Col.vendor) == vendor).then(float(years)).otherwise(vendor_year_expr)
    bucket_expr = pl.lit(0)
    for point in sorted(
        (p for p in config.blending.target_points if p.ep_type == "OEP"),
        key=lambda p: p.return_period,
    ):
        bucket_expr = pl.when(pl.col(Col.rp) >= point.return_period).then(point.return_period).otherwise(bucket_expr)
    return ylt.with_columns(
        pl.col(Col.loss)
        .cum_count()
        .over(*partition_keys)
        .cast(pl.Int64)
        .alias(Col.rnk)
    ).with_columns(
        (vendor_year_expr / pl.col(Col.rnk))
        .alias(Col.rp)
    ).with_columns(
        bucket_expr.alias(Col.rp_bucket)
    )


def apply_ep_blending_to_ylt(
    ylt: pl.LazyFrame,
    ep_blending_targets: pl.LazyFrame,
) -> pl.LazyFrame:
    factors = ep_blending_targets.select(
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.blend_subregion_peril_id,
        pl.col(Col.return_period).alias(Col.rp_bucket),
        Col.ep_type,
        Col.risklink_loss,
        Col.verisk_loss,
        Col.risklink_blended_contribution,
        Col.verisk_blended_contribution,
        Col.target_loss,
        Col.base_model,
        Col.base_model_loss,
        Col.uplift_factor_on_base_model,
    )

    diagnostic_cols = [
        Col.risklink_blended_contribution,
        Col.verisk_blended_contribution,
        Col.uplift_factor_on_base_model,
    ]
    return (
        ylt.join(
            factors,
            on=[
                Col.rollup_lob,
                Col.rollup_peril,
                Col.region_peril_id,
                Col.blend_subregion_peril_id,
                Col.rp_bucket,
                Col.base_model,
            ],
            how="inner",
        )
        .with_columns(
            (pl.col(Col.loss) * pl.col(Col.uplift_factor_on_base_model)).alias(Col.loss),
            pl.lit("blended").alias(Col.metric),
        )
        .drop(
            Col.ep_type,
            Col.risklink_loss,
            Col.verisk_loss,
            Col.target_loss,
            Col.base_model_loss,
            strict=False,
        )
        .select(pl.all().exclude(diagnostic_cols), *diagnostic_cols)
    )


def convert_ylt_to_local_currency(
    ylt_blended: pl.LazyFrame,
    fx_rates: pl.LazyFrame,
) -> pl.LazyFrame:
    return (
        ylt_blended.join(fx_rates, on=Col.currency, how="inner")
        .with_columns(
            (pl.col(Col.loss) / pl.col(Col.fx_rate)).alias(Col.loss),
            pl.col(Col.currency).alias(Col.target_currency),
            pl.lit("localccy").alias(Col.metric),
        )
        .drop(Col.fx_rate_date, Col.fx_rate)
    )


def apply_forecast_factors_to_ylt(
    ylt_localccy: pl.LazyFrame,
    forecast_dates: pl.LazyFrame,
    forecast_factors: pl.LazyFrame,
) -> pl.LazyFrame:
    return (
        ylt_localccy.join(forecast_dates, how="cross")
        .join(
            forecast_factors,
            on=[Col.class_, Col.office, Col.forecast_date],
            how="left",
        )
        .with_columns(
            (pl.col(Col.loss) * pl.col("_forecast_factor_raw").fill_null(1.0)).alias(Col.loss),
            pl.lit("localccy_forecast").alias(Col.metric),
        )
        .drop("_forecast_factor_raw")
    )


def apply_euws_factors_to_ylt(
    ylt_forecasted: pl.LazyFrame,
    verisk_events: pl.LazyFrame,
    seeds: dict[str, pl.LazyFrame],
) -> pl.LazyFrame:
    euws_factors = seeds["euws_rate_factors"].select(
        Col.model_event_id,
        pl.col(RawCol.factor).alias("_euws_factor_raw_source"),
    )
    return (
        ylt_forecasted.join(
            verisk_events,
            on=[Col.event_id, Col.year_id, Col.model_code],
            how="left",
        )
        .join(euws_factors, on=Col.model_event_id, how="left")
        .with_columns(
            pl.col("_euws_factor_raw_source").fill_null(1.0).alias("_euws_factor_raw")
        )
        .with_columns(
            pl.col(Col.loss).alias("_localccy_forecast_loss"),
            (pl.col(Col.loss) * pl.col("_euws_factor_raw")).alias(Col.loss),
            pl.lit("euws").alias(Col.metric),
        )
        .drop("_euws_factor_raw_source")
    )


def apply_euws_overrides_to_ylt(
    ylt_euws_raw: pl.LazyFrame,
    seeds: dict[str, pl.LazyFrame],
) -> pl.LazyFrame:
    euws_overrides = seeds["euws_rank_overrides"].select(
        Col.rollup_lob,
        pl.col(RawCol.max_rank).alias("_euws_override_max_rank"),
        pl.col(RawCol.factor).alias("_euws_override_factor"),
    )
    override_condition = (
        pl.col("_euws_override_factor").is_not_null()
        & (pl.col(Col.rnk) <= pl.col("_euws_override_max_rank"))
        & (pl.col("_euws_factor_raw") == 0)
    )
    return (
        ylt_euws_raw.join(euws_overrides, on=Col.rollup_lob, how="left")
        .with_columns(
            pl.when(override_condition)
            .then(pl.col("_euws_override_factor"))
            .otherwise(pl.col("_euws_factor_raw"))
            .alias("_euws_factor")
        )
        .with_columns(
            pl.when(override_condition)
            .then(pl.col("_localccy_forecast_loss") * pl.col("_euws_override_factor"))
            .otherwise(pl.col(Col.loss))
            .alias(Col.loss),
            pl.lit("euws_override").alias(Col.metric),
        )
        .drop("_euws_override_max_rank", "_euws_override_factor", "_euws_factor")
    )
