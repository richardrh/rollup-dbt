"""Pipeline mart model query functions."""

from __future__ import annotations

import polars as pl


def summarize_losses(selected_losses: pl.LazyFrame) -> pl.LazyFrame:
    """Aggregate selected losses by vendor and analysis."""

    return (
        selected_losses.group_by("vendor", "analysis_id")
        .agg(
            pl.col("loss").sum().alias("total_loss"),
            pl.len().cast(pl.UInt32).alias("event_count"),
        )
        .sort("vendor", "analysis_id")
    )


def build_analysis_loss_summary(adjusted_losses: pl.LazyFrame) -> pl.LazyFrame:
    """Aggregate adjusted losses by business analysis dimensions."""

    return (
        adjusted_losses.group_by("vendor", "analysis_id", "peril_id", "peril_name", "rollup_lob", "currency")
        .agg(
            pl.col("loss").sum().alias("total_loss"),
            pl.col("adjusted_loss").sum().alias("adjusted_total_loss"),
            pl.len().cast(pl.UInt32).alias("event_count"),
        )
        .sort("vendor", "analysis_id", "peril_id", "rollup_lob")
    )


def build_event_loss_fanout(adjusted_losses: pl.LazyFrame) -> pl.LazyFrame:
    """Create event-level fanout mart rows for downstream output tables."""

    return adjusted_losses.select(
        "vendor",
        "analysis_id",
        "year_id",
        "event_id",
        "peril_id",
        "peril_name",
        "region",
        "peril_family",
        "rollup_lob",
        "currency",
        "loss",
        "forecast_factor",
        "fx_rate",
        "euws_rate_factor",
        "adjusted_loss",
    ).sort("vendor", "analysis_id", "year_id", "event_id")


def build_blended_loss_summary(blended_losses: pl.LazyFrame) -> pl.LazyFrame:
    """Create blended vendor/peril fanout mart rows from VOR weights."""

    return (
        blended_losses.group_by("peril_id", "peril_name", "vendor", "base_model")
        .agg(
            pl.col("blend_weight").max().alias("blend_weight"),
            pl.col("adjusted_loss").sum().alias("adjusted_total_loss"),
            pl.col("blended_loss").sum().alias("blended_total_loss"),
        )
        .sort("peril_id", "vendor")
    )


def build_loss_summary(selected_losses: pl.LazyFrame) -> pl.LazyFrame:
    """Create the final fanout loss-summary mart output."""

    return summarize_losses(selected_losses)
