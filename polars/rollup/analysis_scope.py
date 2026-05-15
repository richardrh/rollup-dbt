"""Resolve the effective analysis run scope.

When ``business/selected_analyses.csv`` exists it is the operator-facing source
of truth. RiskLink selections use the numeric ``analysis_id``. Verisk
selections use the EP/YLT ``Analysis`` label, stored in ``analyses.modelled_label``.
``valid_analyses.csv`` remains only as the legacy compatibility path when the
selected-analysis seed is absent.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from rollup.config import VendorName
from rollup.schemas import frames as F
from rollup.schemas.columns import AnalysesCol as AN
from rollup.schemas.columns import SelectedAnalysesCol as SA
from rollup.schemas.columns import ValidAnalysesCol as VA


SELECTED_ANALYSIS_ID = "selected_analysis_id"


class SelectedAnalysisResolutionError(ValueError):
    """An enabled selected-analysis row could not resolve to ``analyses.csv``."""


def selected_analyses_path(seeds_dir: Path) -> Path:
    return seeds_dir / "business" / "selected_analyses.csv"


def has_selected_analyses_seed(seeds_dir: Path) -> bool:
    return selected_analyses_path(seeds_dir).exists()


def _enabled_selected(path: Path) -> pl.LazyFrame:
    return (
        pl.scan_csv(path, schema=F.SELECTED_ANALYSES)
        .filter(pl.col(SA.INCLUDE))
        .select(
            pl.col(SA.VENDOR),
            pl.col(SA.ANALYSIS_ID).alias(SELECTED_ANALYSIS_ID),
        )
        .unique()
    )


def _with_selected_ids_from_selected(
    analyses: pl.LazyFrame,
    selected: pl.LazyFrame,
) -> pl.LazyFrame:
    selected_risklink = selected.filter(pl.col(SA.VENDOR) == VendorName.RISKLINK.value)
    selected_verisk = selected.filter(pl.col(SA.VENDOR) == VendorName.VERISK.value)

    risklink = analyses.join(
        selected_risklink,
        left_on=[AN.VENDOR, AN.ANALYSIS_ID],
        right_on=[SA.VENDOR, SELECTED_ANALYSIS_ID],
        how="inner",
    ).with_columns(pl.col(AN.ANALYSIS_ID).alias(SELECTED_ANALYSIS_ID))
    verisk = analyses.join(
        selected_verisk,
        left_on=[AN.VENDOR, AN.MODELLED_LABEL],
        right_on=[SA.VENDOR, SELECTED_ANALYSIS_ID],
        how="inner",
    ).with_columns(pl.col(AN.MODELLED_LABEL).alias(SELECTED_ANALYSIS_ID))
    return pl.concat([risklink, verisk], how="diagonal").select(
        AN.VENDOR,
        AN.ANALYSIS_ID,
        AN.MODELLED_LABEL,
        AN.PERIL_ID,
        AN.LOB_ID,
        SELECTED_ANALYSIS_ID,
    )


def analyses_with_selected_ids_for_run(
    seeds_dir: Path,
    analyses: pl.LazyFrame,
    valid_analyses: pl.LazyFrame,
    *,
    validate_selected: bool = True,
) -> pl.LazyFrame:
    """Return analysis metadata for the effective run scope.

    The returned frame includes ``selected_analysis_id`` so dry-run checks can
    render the exact analyst-facing key. Runtime callers normally drop that
    helper column via :func:`analyses_for_run`.
    """
    path = selected_analyses_path(seeds_dir)
    if not path.exists():
        valid = valid_analyses.select(
            pl.col(VA.VENDOR),
            pl.col(VA.ANALYSIS_ID),
        ).unique()
        return analyses.join(
            valid,
            left_on=[AN.VENDOR, AN.ANALYSIS_ID],
            right_on=[VA.VENDOR, VA.ANALYSIS_ID],
            how="inner",
        ).with_columns(pl.col(AN.ANALYSIS_ID).alias(SELECTED_ANALYSIS_ID))

    selected = _enabled_selected(path)
    resolved = _with_selected_ids_from_selected(analyses, selected)
    if validate_selected:
        selected_df = selected.collect()
        resolved_keys = resolved.select(AN.VENDOR, SELECTED_ANALYSIS_ID).unique().collect()
        missing = selected_df.join(resolved_keys, on=[SA.VENDOR, SELECTED_ANALYSIS_ID], how="anti")
        if missing.height:
            examples = missing.head(10).rows()
            raise SelectedAnalysisResolutionError(
                "selected_analyses.csv contains enabled analysis IDs that do not resolve "
                f"in analyses.csv: {examples}"
            )
    return resolved


def analyses_for_run(
    seeds_dir: Path,
    analyses: pl.LazyFrame,
    valid_analyses: pl.LazyFrame,
    *,
    validate_selected: bool = True,
) -> pl.LazyFrame:
    """Return ``analyses.csv`` rows for the effective runtime run scope."""
    return analyses_with_selected_ids_for_run(
        seeds_dir,
        analyses,
        valid_analyses,
        validate_selected=validate_selected,
    ).select(
        AN.VENDOR,
        AN.ANALYSIS_ID,
        AN.MODELLED_LABEL,
        AN.PERIL_ID,
        AN.LOB_ID,
    )
