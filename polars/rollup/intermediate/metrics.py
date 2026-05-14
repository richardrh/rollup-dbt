"""Intermediate metric transformations."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

from rollup.chain import CHAIN, CHAIN_BASE, col_after, factor_col_for
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import MetricCol as M
from rollup.schemas.columns import NormalizedYltCol as Y


def add_main_metrics(ylt: pl.LazyFrame, tags: Sequence[str]) -> pl.LazyFrame:
    """Add year-invariant chain columns and year-tagged MAIN chain columns."""
    ylt = ylt.with_columns(
        (pl.col(Y.LOSS) * pl.col(AF.UPLIFT_FACTOR)).alias(M.LOSS_UPLIFTED),
        (pl.col(Y.LOSS) * pl.col(AF.UPLIFT_FACTOR_CAPPED)).alias(M.LOSS_UPLIFTED_CAPPED),
    ).with_columns(
        (pl.col(M.LOSS_UPLIFTED_CAPPED) / pl.col(AF.RATE_TO_GBP)).alias(CHAIN_BASE),
    )

    prev_for: dict[str, str] = {tag: CHAIN_BASE for tag in tags}
    for stage_name, stage in CHAIN.items():
        exprs = [
            (pl.col(prev_for[tag]) * pl.col(factor_col_for(stage, tag))).alias(col_after(stage_name, tag))
            for tag in tags
        ]
        ylt = ylt.with_columns(exprs)
        for tag in tags:
            prev_for[tag] = col_after(stage_name, tag)
    return ylt


def add_dialsup(ylt: pl.LazyFrame, forecast_tag: str) -> pl.LazyFrame:
    """Add the tag-independent DIALSUP sensitivity metric.

    ``dialsup = loss * forecast * euws``

    The DIALSUP fanout is intentionally a single output, so it uses the first
    forecast tag chosen for that fanout rather than emitting one metric per tag.
    It bypasses uplift, uplift cap, and FX/local-currency conversion.
    """
    from rollup.chain import DIALSUP_COL, forecast_factor_col

    return ylt.with_columns(
        (
            pl.col(Y.LOSS)
            * pl.col(forecast_factor_col(forecast_tag))
            * pl.col(AF.EUWS_FACTOR)
        ).alias(DIALSUP_COL),
    )
