from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol
from rollup.staging import StagingFrames


def build_enriched_ylt(normalized_ylt: pl.LazyFrame, staged_ep: pl.LazyFrame) -> pl.LazyFrame:
    ep_keys = staged_ep.select(
        Col.vendor,
        Col.analysis_id,
        Col.modelled_lob,
        Col.modelled_peril,
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.class_,
        Col.office,
        Col.currency,
        Col.selection_priority,
        Col.is_dialsup,
    ).unique()
    verisk_keys = ep_keys.filter(pl.col(Col.vendor) == "verisk").drop(Col.analysis_id)
    risklink_keys = ep_keys.filter(pl.col(Col.vendor) == "risklink").drop(
        [Col.modelled_lob, Col.modelled_peril]
    )

    verisk = normalized_ylt.filter(pl.col(Col.vendor) == "verisk").join(
        verisk_keys,
        on=[Col.vendor, Col.modelled_lob, Col.modelled_peril],
        how="inner",
    )
    risklink = normalized_ylt.filter(pl.col(Col.vendor) == "risklink").join(
        risklink_keys,
        on=[Col.vendor, Col.analysis_id],
        how="inner",
    )
    return pl.concat([verisk, risklink], how="diagonal_relaxed")


def apply_adjustments(enriched: pl.LazyFrame, frames: StagingFrames) -> pl.LazyFrame:
    with_blending = _apply_blending(enriched, frames.blending)
    with_fx = _apply_fx(with_blending, frames.fx_rates)
    with_forecast = _apply_forecast(with_fx, frames.forecast_factors)
    return _apply_euws(with_forecast, frames.euws_factors)


def _apply_blending(enriched: pl.LazyFrame, blending: pl.DataFrame) -> pl.LazyFrame:
    if blending.is_empty():
        return enriched.with_columns(pl.col(Col.loss).alias("blended_loss"))
    columns = blending.columns
    region_col = RawCol.RegionPerilID if RawCol.RegionPerilID in columns else Col.region_peril_id
    air_col = RawCol.AIRBlend if RawCol.AIRBlend in columns else "verisk_weight"
    rms_col = RawCol.RMSBlend if RawCol.RMSBlend in columns else "risklink_weight"
    weights = blending.lazy().select(
        pl.col(region_col).cast(pl.Int64).alias(Col.region_peril_id),
        _optional_weight(air_col, columns).alias("verisk_blend_weight"),
        _optional_weight(rms_col, columns).alias("risklink_blend_weight"),
    )
    return enriched.join(weights, on=Col.region_peril_id, how="left").with_columns(
        pl.when(pl.col(Col.vendor) == "verisk")
        .then(pl.col("verisk_blend_weight"))
        .otherwise(pl.col("risklink_blend_weight"))
        .fill_null(1.0)
        .alias("blend_weight")
    ).with_columns((pl.col(Col.loss) * pl.col("blend_weight")).alias("blended_loss"))


def _apply_fx(frame: pl.LazyFrame, fx_rates: pl.DataFrame) -> pl.LazyFrame:
    if fx_rates.is_empty():
        return frame.with_columns(pl.lit(1.0).alias(Col.fx_rate), pl.col("blended_loss").alias("gbp_loss"))
    columns = fx_rates.columns
    currency_col = RawCol.currency_code if RawCol.currency_code in columns else Col.currency
    rates = fx_rates.lazy().select(
        pl.col(currency_col).cast(pl.String).alias(Col.currency),
        pl.col(RawCol.rate).cast(pl.Float64).alias(Col.fx_rate),
    ).unique(Col.currency, keep="last")
    return frame.join(rates, on=Col.currency, how="left").with_columns(
        pl.col(Col.fx_rate).fill_null(1.0),
    ).with_columns((pl.col("blended_loss") * pl.col(Col.fx_rate)).alias("gbp_loss"))


def _apply_forecast(frame: pl.LazyFrame, forecast_factors: pl.DataFrame) -> pl.LazyFrame:
    if forecast_factors.is_empty():
        return frame.with_columns(
            pl.lit("base").alias(Col.forecast_date),
            pl.lit(1.0).alias(Col.forecast_factor),
            pl.col("gbp_loss").alias("forecast_loss"),
        )
    factors = forecast_factors.lazy().select(
        pl.col(Col.class_).cast(pl.String),
        pl.col(Col.office).cast(pl.String),
        pl.col(Col.forecast_date).cast(pl.String),
        pl.col(RawCol.factor).cast(pl.Float64).alias(Col.forecast_factor),
    )
    return frame.join(factors, on=[Col.class_, Col.office], how="left").with_columns(
        pl.col(Col.forecast_date).fill_null("base"),
        pl.col(Col.forecast_factor).fill_null(1.0),
    ).with_columns((pl.col("gbp_loss") * pl.col(Col.forecast_factor)).alias("forecast_loss"))


def _apply_euws(frame: pl.LazyFrame, euws_factors: pl.DataFrame) -> pl.LazyFrame:
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


def build_metric_long(adjusted: pl.LazyFrame) -> pl.LazyFrame:
    base = adjusted.select(
        Col.vendor,
        pl.col(Col.vendor).alias(Col.base_model),
        Col.analysis_id,
        Col.modelled_lob,
        Col.modelled_peril,
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.class_,
        Col.office,
        Col.currency,
        Col.year_id,
        Col.event_id,
        Col.forecast_date,
        Col.is_dialsup,
        Col.loss,
        "blended_loss",
        "gbp_loss",
        "forecast_loss",
        "euws_loss",
    )
    return pl.concat(
        [
            _metric(base, Col.loss, "original_ylt_loss"),
            _metric(base, "blended_loss", "blended"),
            _metric(base, "gbp_loss", "gbp"),
            _metric(base, "forecast_loss", "forecast"),
            _metric(base, "euws_loss", "euws_override"),
        ],
        how="vertical",
    )


def _metric(frame: pl.LazyFrame, source_col: str, metric: str) -> pl.LazyFrame:
    return frame.select(
        Col.vendor,
        Col.base_model,
        Col.analysis_id,
        Col.modelled_lob,
        Col.modelled_peril,
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.class_,
        Col.office,
        Col.currency,
        Col.year_id,
        Col.event_id,
        Col.forecast_date,
        Col.is_dialsup,
        pl.lit(metric).alias(Col.metric),
        pl.col(source_col).cast(pl.Float64).alias(Col.loss),
    )


def build_dialsup(metric_long: pl.LazyFrame) -> pl.LazyFrame:
    return metric_long.filter(
        (pl.col(Col.is_dialsup) == 1) & (pl.col(Col.metric) == "forecast")
    ).with_columns(pl.lit("dialsup_gbp_forecast").alias(Col.metric))


def _optional_weight(column: str, columns: list[str]) -> pl.Expr:
    if column in columns:
        return pl.col(column).cast(pl.Float64).fill_null(1.0)
    return pl.lit(1.0)
