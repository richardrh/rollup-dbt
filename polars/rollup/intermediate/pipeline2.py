"""Pipeline2 intermediate model query functions."""

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
