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


def build_loss_summary(selected_losses: pl.LazyFrame) -> pl.LazyFrame:
    """Create the final fanout loss-summary mart output."""

    return summarize_losses(selected_losses)
