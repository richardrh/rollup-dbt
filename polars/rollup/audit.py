"""Audit projections for all-factors frames."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

from rollup.chain import CHAIN, CHAIN_BASE, DIALSUP_COL, audit_layout_cols, col_after
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import MetricCol as M
from rollup.schemas.columns import NormalizedYltCol as Y


_LOSS_RAW_COL = "loss_raw"
_METRIC_NAME_COL = "metric_name"
_METRIC_VALUE_COL = "value"

_IDENTITY_COLS: tuple[str, ...] = (
    AF.VENDOR, AF.LOB_ID, AF.MODELLED_LOB, AF.ROLLUP_LOB, AF.LOB_TYPE,
    AF.CDS_CAT_CLASS_NAME,
    AF.REGION_PERIL_ID, AF.MODELLED_REGION_PERIL,
    AF.PERIL_NAME, AF.REGION, AF.PERIL_FAMILY,
    AF.YEAR_ID, AF.EVENT_ID, AF.MODEL_EVENT_ID, AF.MODEL_CODE,
    AF.RNK, AF.RP, AF.RP_BUCKET,
    AF.RL_PROPORTION, AF.VK_PROPORTION, AF.BASE_MODEL,
)


def _metric_cols_for(tags: Sequence[str]) -> list[str]:
    """Every metric column this pipeline produces — driven by `chain.CHAIN`."""
    cols: list[str] = [
        Y.LOSS,
        M.LOSS_UPLIFTED, M.LOSS_UPLIFTED_CAPPED, M.LOSS_UPLIFTED_CAPPED_LOCALCCY,
    ]
    for stage_name in CHAIN:
        cols += [col_after(stage_name, tag) for tag in tags]
    cols.append(DIALSUP_COL)
    return cols


def audit_wide(all_factors: pl.LazyFrame, tags: Sequence[str]) -> pl.LazyFrame:
    """One row per event, columns ordered so the factor chain reads left-to-right."""
    seen: set[str] = set(_IDENTITY_COLS)
    cols: list[pl.Expr] = [pl.col(c) for c in _IDENTITY_COLS]
    cols.append(pl.col(Y.LOSS).alias(_LOSS_RAW_COL))

    cols += [
        pl.col(AF.UPLIFT_FACTOR), pl.col(AF.UPLIFT_FACTOR_CAPPED),
        pl.col(M.LOSS_UPLIFTED), pl.col(M.LOSS_UPLIFTED_CAPPED),
        pl.col(AF.REQUIRED_CURRENCY), pl.col(AF.RATE_TO_GBP),
        pl.col(CHAIN_BASE),
    ]

    for c in audit_layout_cols(list(tags)):
        if c not in seen:
            cols.append(pl.col(c))
            seen.add(c)

    cols.append(pl.col(DIALSUP_COL))

    return all_factors.select(cols).sort(
        [Y.VENDOR, Y.LOB_ID, Y.REGION_PERIL_ID, Y.YEAR_ID, Y.EVENT_ID],
    )


def audit_long(
    all_factors: pl.LazyFrame,
    tags: Sequence[str],
    *,
    min_loss: float = 0.0,
) -> pl.LazyFrame:
    """Identity columns + one row per (metric_name, value)."""
    metric_cols = _metric_cols_for(tags)
    out = (
        all_factors
        .select(*[pl.col(c) for c in _IDENTITY_COLS], *[pl.col(c) for c in metric_cols])
        .unpivot(
            on=metric_cols,
            index=list(_IDENTITY_COLS),
            variable_name=_METRIC_NAME_COL,
            value_name=_METRIC_VALUE_COL,
        )
    )
    if min_loss > 0:
        out = out.filter(pl.col(_METRIC_VALUE_COL) >= min_loss)
    return out.sort([Y.VENDOR, Y.LOB_ID, Y.REGION_PERIL_ID, Y.YEAR_ID, Y.EVENT_ID, _METRIC_NAME_COL])
