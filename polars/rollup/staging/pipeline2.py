"""Pipeline2 staging model query functions."""

from __future__ import annotations

import polars as pl


def stage_selected_analyses(selected_analyses: pl.LazyFrame) -> pl.LazyFrame:
    """Project the selected analysis allow-list into its staging shape."""

    return selected_analyses.select(
        pl.col("vendor").cast(pl.String),
        pl.col("analysis_id").cast(pl.String),
    )


def stage_risklink_ylt(raw_risklink_ylt: pl.LazyFrame) -> pl.LazyFrame:
    """Project raw RiskLink YLT rows into the canonical pipeline2 YLT shape."""

    return raw_risklink_ylt.select(
        pl.lit("risklink").alias("vendor"),
        pl.col("anlsid").cast(pl.String).alias("analysis_id"),
        pl.col("yearid").alias("year_id"),
        pl.col("eventid").alias("event_id"),
        pl.col("loss").cast(pl.Float64).alias("loss"),
    )


def stage_verisk_ylt(raw_verisk_ylt: pl.LazyFrame) -> pl.LazyFrame:
    """Project raw Verisk YLT rows into the canonical pipeline2 YLT shape."""

    return raw_verisk_ylt.select(
        pl.lit("verisk").alias("vendor"),
        pl.col("Analysis").cast(pl.String).alias("analysis_id"),
        pl.col("YearID").alias("year_id"),
        pl.col("EventID").alias("event_id"),
        pl.col("GroundUpLoss").cast(pl.Float64).alias("loss"),
    )


def build_normalized_ylt(
    raw_risklink_ylt: pl.LazyFrame,
    raw_verisk_ylt: pl.LazyFrame,
) -> pl.LazyFrame:
    """Union staged vendor YLT models into one normalized YLT model."""

    return pl.concat(
        [stage_risklink_ylt(raw_risklink_ylt), stage_verisk_ylt(raw_verisk_ylt)],
        how="vertical",
    )
