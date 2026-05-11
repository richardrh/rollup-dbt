"""DIALSUP metric.

This module owns the single transformation that produces the ``dialsup``
metric column. Keeping it outside the pipeline orchestrator makes the metric
lineage explicit while preserving the existing formula.
"""

from __future__ import annotations

import polars as pl

from rollup.chain import DIALSUP_COL
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import NormalizedYltCol as Y


def add_dialsup(ylt: pl.LazyFrame) -> pl.LazyFrame:
    """Add the tag-independent DIALSUP sensitivity metric.

    ``dialsup = loss / rate_to_gbp``

    No uplift, no cap, no forecast factor, no euws, no fa_gross. A single
    column ``"dialsup"`` is added — all forecast dates would be identical
    under this definition, so there is no per-tag emission.
    """
    return ylt.with_columns(
        (pl.col(Y.LOSS) / pl.col(AF.RATE_TO_GBP)).alias(DIALSUP_COL),
    )
