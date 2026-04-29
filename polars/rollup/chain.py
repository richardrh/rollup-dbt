"""The factor chain — a registry of multiplicative transformations.

Each entry in `CHAIN` is one stage in the year-tagged chain that produces
the cumulative loss columns:

    loss_uplifted_capped_localccy_{tag}                   ← after `forecast`
    loss_uplifted_capped_localccy_{tag}_euws              ← after `euws`
    loss_uplifted_capped_localccy_{tag}_euws_fagross      ← after `fagross`

The base column the chain starts from is `CHAIN_BASE`
(= `MetricCol.LOSS_UPLIFTED_CAPPED_LOCALCCY` — the year-invariant local-ccy
loss). Each stage takes the previous column and multiplies it by its
`factor_col`. Stage suffixes accumulate left-to-right in the column name —
the column literally tells you which factors have been applied.

Adding a new factor is a one-line edit to `CHAIN`. The metrics computer,
the audit layout, `VariantSpec.loss_metric`, and `_metric_cols_for` all
walk this registry — no other call site hand-builds the column names.

`dialsup` is NOT part of the chain — it's a sensitivity formula
(loss × composite-factor / localccy), not a multiplicative continuation.
Its column-name builder lives here for co-location but it walks zero
registry entries.
"""

from __future__ import annotations

from typing import TypedDict

from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import MetricCol as M


class ChainStage(TypedDict):
    """One multiplicative stage in the year-tagged chain.

    `factor_col` is either a static column name (e.g. `AF.EUWS_FACTOR`) or a
    `{tag}`-templated string (e.g. `"f_{tag}"`) when `is_per_tag` is True.

    `ancillary_before` / `ancillary_after` are non-factor columns that audit
    layout prints alongside the factor (e.g. `RNK` before `EUWS_FACTOR` —
    rank drives the override logic; `FA_GROSS_TAIL_FACTOR` after
    `FA_GROSS_AAL_FACTOR` — same source seed). Empty tuple if none.
    """
    suffix:           str
    factor_col:       str
    is_per_tag:       bool
    ancillary_before: tuple[str, ...]
    ancillary_after:  tuple[str, ...]


# The chain itself, in multiplication order. Insertion order = multiplication
# order = column-suffix order = audit layout order. One source of truth.
CHAIN: dict[str, ChainStage] = {
    "forecast": {
        "suffix": "", "factor_col": "f_{tag}", "is_per_tag": True,
        "ancillary_before": (), "ancillary_after": (),
    },
    "euws": {
        "suffix": "_euws", "factor_col": AF.EUWS_FACTOR, "is_per_tag": False,
        "ancillary_before": (AF.RNK,),                  # rank drives the EUWS override
        "ancillary_after":  (),
    },
    "fagross": {
        "suffix": "_fagross", "factor_col": AF.FA_GROSS_AAL_FACTOR, "is_per_tag": False,
        "ancillary_before": (),
        "ancillary_after":  (AF.FA_GROSS_TAIL_FACTOR,),  # same seed, audit-only today
    },
}


# The year-invariant column the chain multiplies into for each tag.
CHAIN_BASE: str = M.LOSS_UPLIFTED_CAPPED_LOCALCCY


# --------------------------------------------------------------------------- #
# Lookups — every column-name builder routes through these                    #
# --------------------------------------------------------------------------- #

def factor_col_for(stage: ChainStage, tag: str) -> str:
    """Resolve the factor column for `stage` at `tag` (formats per-tag templates)."""
    f = stage["factor_col"]
    return f.format(tag=tag) if stage["is_per_tag"] else f


def col_after(stage_name: str, tag: str) -> str:
    """Cumulative column name after applying stages up to and including `stage_name`."""
    suffixes = ""
    for name, stage in CHAIN.items():
        suffixes += stage["suffix"]
        if name == stage_name:
            return f"{CHAIN_BASE}_{tag}{suffixes}"
    raise KeyError(f"unknown chain stage: {stage_name!r} — known: {list(CHAIN)}")


def main_loss_col(tag: str) -> str:
    """The MAIN flavour's deliverable — column produced by the FINAL chain stage."""
    return col_after(next(reversed(CHAIN)), tag)


def dialsup_col() -> str:
    """Sensitivity column for the DIALSUP flavour. Not part of CHAIN — different formula.

    Returns the literal string ``"dialsup"`` — there is one dialsup column per event,
    not one per forecast tag. All forecast dates would give the same value
    (``loss / rate_to_gbp``) so a single column is emitted.
    """
    return "dialsup"


def forecast_factor_col(tag: str) -> str:
    """The `f_{tag}` column attached by `attach_forecast_factors` —
    resolved through the chain registry so this name has one source of truth."""
    return factor_col_for(CHAIN["forecast"], tag)


def all_chain_cols(tag: str) -> list[str]:
    """Every cumulative chain column for `tag`, in chain order."""
    return [col_after(name, tag) for name in CHAIN]


def audit_layout_cols(tags: list[str]) -> list[str]:
    """Column names for the year-tagged section of `audit_wide`, in left-to-right
    layout order. Per-tag stages emit (factor, metric) per tag inline; static
    stages emit (ancillary_before*, factor, ancillary_after*) once, then their
    metric per tag. Single source of truth for the audit factor-chain layout.

    The ``dialsup`` column is NOT included here — it is appended separately in
    ``audit_wide`` because it is not part of ``CHAIN``.
    """
    out: list[str] = []
    for stage_name, stage in CHAIN.items():
        if stage["is_per_tag"]:
            for tag in tags:
                out.append(factor_col_for(stage, tag))
                out.append(col_after(stage_name, tag))
        else:
            out += list(stage["ancillary_before"])
            out.append(stage["factor_col"])
            out += list(stage["ancillary_after"])
            for tag in tags:
                out.append(col_after(stage_name, tag))
    return out
