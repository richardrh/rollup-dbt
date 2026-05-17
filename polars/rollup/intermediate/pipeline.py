"""Pipeline intermediate model query functions."""

from __future__ import annotations

import polars as pl


def select_losses(
    normalized_ylt: pl.LazyFrame,
    selected_analyses: pl.LazyFrame,
) -> pl.LazyFrame:
    """Filter normalized YLT losses to the selected analysis allow-list."""

    return normalized_ylt.join(
        selected_analyses,
        on=["vendor", "analysis_id"],
        how="inner",
    )


def enrich_losses(
    selected_losses: pl.LazyFrame,
    analyses: pl.LazyFrame,
    perils: pl.LazyFrame,
    lobs: pl.LazyFrame,
) -> pl.LazyFrame:
    """Join selected losses to business analysis, peril, and LOB dimensions."""

    analysis_dimension = analyses.select(
        pl.col("vendor"),
        pl.col("analysis_id"),
        pl.col("modelled_label"),
        pl.col("peril_id"),
        pl.col("lob_id"),
    )
    peril_dimension = perils.select(
        pl.col("peril_id"),
        pl.col("name").alias("peril_name"),
        pl.col("region"),
        pl.col("peril_family"),
    )
    lob_dimension = lobs.select(
        pl.col("lob_id"),
        pl.col("modelled_lob"),
        pl.col("rollup_lob"),
        pl.col("lob_type"),
        pl.col("cds_cat_class_name"),
        pl.col("office"),
        pl.col("class"),
        pl.col("currency"),
    )

    return (
        selected_losses.join(analysis_dimension, on=["vendor", "analysis_id"], how="left")
        .join(peril_dimension, on="peril_id", how="left")
        .join(lob_dimension, on="lob_id", how="left")
    )


def apply_vor_adjustments(
    enriched_losses: pl.LazyFrame,
    forecast_factors: pl.LazyFrame,
    fx_rates: pl.LazyFrame,
    euws_rate_factors: pl.LazyFrame,
) -> pl.LazyFrame:
    """Apply available forecast, FX, and EUWS event factors to enriched losses."""

    forecast = forecast_factors.select(
        pl.col("class"),
        pl.col("office"),
        pl.col("factor").alias("forecast_factor"),
    )
    fx = fx_rates.filter(pl.col("target_currency") == "GBP").select(
        pl.col("currency_code").alias("currency"),
        pl.col("rate").alias("fx_rate"),
    )
    euws = euws_rate_factors.select(
        pl.col("model_event_id").alias("event_id"),
        pl.col("occ_year").alias("year_id"),
        pl.col("factor").alias("euws_rate_factor"),
    )

    return (
        enriched_losses.join(forecast, on=["class", "office"], how="left")
        .join(fx, on="currency", how="left")
        .join(euws, on=["event_id", "year_id"], how="left")
        .with_columns(
            pl.coalesce(pl.col("forecast_factor"), pl.lit(1.0)).alias("forecast_factor"),
            pl.coalesce(pl.col("fx_rate"), pl.lit(1.0)).alias("fx_rate"),
            pl.coalesce(pl.col("euws_rate_factor"), pl.lit(1.0)).alias("euws_rate_factor"),
        )
        .with_columns(
            (
                pl.col("loss")
                * pl.col("forecast_factor")
                * pl.col("fx_rate")
                * pl.col("euws_rate_factor")
            ).alias("adjusted_loss")
        )
    )


def blend_losses(adjusted_losses: pl.LazyFrame, blending_weights: pl.LazyFrame) -> pl.LazyFrame:
    """Apply available return-period-zero vendor blending weights to adjusted losses."""

    weights = blending_weights.filter(pl.col("return_period") == 0).select(
        pl.col("peril_id"),
        pl.col("vendor"),
        pl.col("base_model"),
        pl.col("weight").alias("blend_weight"),
    )

    return (
        adjusted_losses.join(weights, on=["peril_id", "vendor"], how="left")
        .with_columns(pl.coalesce(pl.col("blend_weight"), pl.lit(1.0)).alias("blend_weight"))
        .with_columns((pl.col("adjusted_loss") * pl.col("blend_weight")).alias("blended_loss"))
    )


def build_selected_losses(
    normalized_ylt: pl.LazyFrame,
    selected_analyses: pl.LazyFrame,
) -> pl.LazyFrame:
    """Run the selected-analysis business join."""

    return select_losses(normalized_ylt, selected_analyses)


def build_intermediate_losses(
    normalized_ylt: pl.LazyFrame,
    selected_analyses: pl.LazyFrame,
    analyses: pl.LazyFrame,
    perils: pl.LazyFrame,
    lobs: pl.LazyFrame,
    forecast_factors: pl.LazyFrame,
    fx_rates: pl.LazyFrame,
    euws_rate_factors: pl.LazyFrame,
    blending_weights: pl.LazyFrame,
) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
    """Build selected, enriched, adjusted, and blended intermediate loss frames."""

    selected_losses = select_losses(normalized_ylt, selected_analyses)
    enriched = enrich_losses(selected_losses, analyses, perils, lobs)
    adjusted = apply_vor_adjustments(enriched, forecast_factors, fx_rates, euws_rate_factors)
    blended = blend_losses(adjusted, blending_weights)
    return selected_losses, enriched, adjusted, blended
