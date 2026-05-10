"""Derive per-peril blending weights from long-format EP-summary CSVs.

Pipeline today reads `blending_weights.csv` (a hand-curated seed with
50/50 placeholder values). This module produces the same shape from the
EP summaries by computing each vendor's AAL per peril and turning that
into a proportion.

Formula:
    rl_aal      = sum(gl)   where ep_type='AAL' and rows mapped to risklink
    vk_aal      = sum(gl)   where ep_type='AAL' and rows mapped to verisk
    rl_prop     = rl_aal / (rl_aal + vk_aal)     (when total > 0)
    vk_prop     = 1 - rl_prop

Returns one row per peril x vendor: peril_id, peril_name, description,
sub_peril (always None today), vendor, weight.
"""
from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from rollup.config import VendorName
from rollup.schemas.columns import (
    AnalysesCol as AN,
    BlendingWeightsCol as BW,
    EpType,
    PerilsCol as P,
    StgRisklinkEpCol as RL,
    StgVeriskEpCol as VK,
)


# Working / temporary column names — exist only inside the blending computation.
# Module-level so every reference is traceable from pl.col(...).
_PERIL_ID_TMP    = "peril_id"
_VENDOR_AAL_TMP  = "vendor_aal"
_RL_AAL_TMP      = "rl_aal"
_VK_AAL_TMP      = "vk_aal"
_TOTAL_TMP       = "total"
_RL_PROP_TMP     = "rl_proportion"
_VK_PROP_TMP     = "vk_proportion"
_PERIL_NAME_TMP  = "peril_name"


log = logging.getLogger("rollup.blending")


def _aal_by_peril(
    long_csvs: list[Path],
    vendor: VendorName,
    analyses: pl.DataFrame,
) -> pl.DataFrame:
    """Read every long-format EP CSV for a vendor, filter to AAL rows, map
    `region_peril` → `peril_id`, sum AAL per peril across all files.

    Returns DataFrame[peril_id: Int64, vendor_aal: Float64].
    Missing region_peril mappings are warned and skipped.
    Empty `long_csvs` (vendor not yet delivered) returns a 0-row frame.
    """
    if not long_csvs:
        log.warning(f"{vendor.value}: no EP long CSVs found — vendor AAL will be 0")
        return pl.DataFrame(schema={_PERIL_ID_TMP: pl.Int64, _VENDOR_AAL_TMP: pl.Float64})

    ep = pl.concat([pl.scan_csv(p) for p in long_csvs], how="vertical_relaxed").collect()

    if vendor == VendorName.RISKLINK:
        peril_label_col = RL.REGION_PERIL
    elif vendor == VendorName.VERISK:
        # Verisk uses 'analysis' instead of 'region_peril' as the peril label.
        peril_label_col = VK.ANALYSIS if VK.ANALYSIS in ep.columns else "analysis"
    else:
        raise ValueError(f"unknown vendor {vendor!r}")

    aal_rows = ep.filter(pl.col(RL.EP_TYPE) == EpType.AAL)

    # Map peril label -> peril_id via analyses (filter to this vendor).
    label_to_pid = (
        analyses
        .filter(pl.col(AN.VENDOR) == vendor.value)
        .select(
            pl.col(AN.MODELLED_LABEL).alias(peril_label_col),
            pl.col(AN.PERIL_ID),
        )
        .unique()
    )

    joined = aal_rows.join(label_to_pid, on=peril_label_col, how="left")

    # Warn for unmapped labels.
    unmapped = joined.filter(pl.col(AN.PERIL_ID).is_null())
    if unmapped.height > 0:
        bad_labels = unmapped[peril_label_col].unique().sort().to_list()
        log.warning(
            f"{vendor.value}: {len(bad_labels)} EP-summary labels not in "
            f"analyses.csv: {bad_labels}"
        )

    return (
        joined
        .filter(pl.col(AN.PERIL_ID).is_not_null())
        .group_by(AN.PERIL_ID)
        .agg(pl.col(RL.GL).sum().alias(_VENDOR_AAL_TMP))
        .rename({AN.PERIL_ID: _PERIL_ID_TMP})
    )


def derive_blending_weights(
    rl_long_csvs: list[Path],
    vk_long_csvs: list[Path],
    analyses: pl.DataFrame,
    perils: pl.DataFrame,
) -> pl.DataFrame:
    """Compute long-format blending_weights from EP-summary AALs.

    Returns one row per (peril_id, vendor); peril_name and description are
    populated from `perils`. `sub_peril` is None. `weight` is the proportion.
    """
    rl_aal = _aal_by_peril(rl_long_csvs, VendorName.RISKLINK, analyses)
    vk_aal = _aal_by_peril(vk_long_csvs, VendorName.VERISK, analyses)

    rl_aal = rl_aal.rename({_VENDOR_AAL_TMP: _RL_AAL_TMP})
    vk_aal = vk_aal.rename({_VENDOR_AAL_TMP: _VK_AAL_TMP})

    # Outer join so any peril seen by either vendor is represented.
    joined = rl_aal.join(vk_aal, on=_PERIL_ID_TMP, how="full", coalesce=True)

    proportions = (
        joined
        .with_columns(
            pl.col(_RL_AAL_TMP).fill_null(0.0),
            pl.col(_VK_AAL_TMP).fill_null(0.0),
        )
        .with_columns(
            **{_TOTAL_TMP: pl.col(_RL_AAL_TMP) + pl.col(_VK_AAL_TMP)},
        )
        .with_columns(
            **{_RL_PROP_TMP: pl.when(pl.col(_TOTAL_TMP) > 0)
               .then(pl.col(_RL_AAL_TMP) / pl.col(_TOTAL_TMP))
               .otherwise(pl.lit(0.5))},
        )
        .with_columns(**{_VK_PROP_TMP: pl.lit(1.0) - pl.col(_RL_PROP_TMP)})
        .join(
            perils.select(P.PERIL_ID, P.NAME).rename(
                {P.PERIL_ID: _PERIL_ID_TMP, P.NAME: _PERIL_NAME_TMP}
            ),
            on=_PERIL_ID_TMP,
            how="left",
        )
    )

    # Long-format: one row per (peril_id, vendor).
    rl_rows = proportions.select(
        pl.col(_PERIL_ID_TMP).alias(BW.PERIL_ID),
        pl.col(_PERIL_NAME_TMP).alias(BW.PERIL_NAME),
        pl.lit("derived from EP-summary AALs").alias(BW.DESCRIPTION),
        pl.lit(None, dtype=pl.String).alias(BW.SUB_PERIL),
        pl.lit(VendorName.RISKLINK.value).alias(BW.VENDOR),
        pl.col(_RL_PROP_TMP).alias(BW.WEIGHT),
    )
    vk_rows = proportions.select(
        pl.col(_PERIL_ID_TMP).alias(BW.PERIL_ID),
        pl.col(_PERIL_NAME_TMP).alias(BW.PERIL_NAME),
        pl.lit("derived from EP-summary AALs").alias(BW.DESCRIPTION),
        pl.lit(None, dtype=pl.String).alias(BW.SUB_PERIL),
        pl.lit(VendorName.VERISK.value).alias(BW.VENDOR),
        pl.col(_VK_PROP_TMP).alias(BW.WEIGHT),
    )
    out = pl.concat([rl_rows, vk_rows]).sort([BW.PERIL_ID, BW.VENDOR])  # BW.PERIL_ID uses enum
    if out.height == 0:
        raise ValueError(
            "derive_blending_weights produced 0 rows — every EP-summary "
            "label was unmapped. Check `analyses.modelled_label` covers "
            "the labels seen in the long CSVs."
        )
    return out
