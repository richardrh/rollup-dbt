"""DIALSUP metric.

This module owns the single transformation that produces the ``dialsup``
metric column. Keeping it outside the pipeline orchestrator makes the metric
lineage explicit.
"""

from __future__ import annotations

import polars as pl

from rollup.chain import DIALSUP_COL, forecast_factor_col
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import NormalizedYltCol as Y


def add_dialsup(ylt: pl.LazyFrame, forecast_tag: str) -> pl.LazyFrame:
    """Add the tag-independent DIALSUP sensitivity metric.

    ``dialsup = loss * forecast * euws * fa_gross``

    The DIALSUP fanout is intentionally a single output, so it uses the first
    forecast tag chosen for that fanout rather than emitting one metric per tag.
    It bypasses uplift, uplift cap, and FX/local-currency conversion.
    """
    return ylt.with_columns(
        (
            pl.col(Y.LOSS)
            * pl.col(forecast_factor_col(forecast_tag))
            * pl.col(AF.EUWS_FACTOR)
            * pl.col(AF.FA_GROSS_AAL_FACTOR)
        ).alias(DIALSUP_COL),
    )
