"""Derive per-peril blending weights from long-format EP-summary CSVs.

Pipeline reads `blending_weights.csv` (a hand-curated seed with 50/50
placeholder values). This module produces the same shape from the EP
summaries by computing each vendor's EP loss per peril at the AAL,
1-in-200, and 1-in-1000 return-period buckets, then turning those values
into proportions.

Formula (per peril_id, return_period):
    rl_aal   = sum(gl) where ep_type in {'AAL', 'OEP'} and rp in {0, 200, 1000}
    vk_aal   = sum(gl) where ep_type in {'AAL', 'OEP'} and rp in {0, 200, 1000}
    rl_prop  = rl_aal / (rl_aal + vk_aal)   (when total > 0)
    vk_prop  = 1 - rl_prop

Returns one row per (peril_id, return_period, vendor): peril_id, return_period,
peril_name, description, sub_peril (always None today), vendor, weight.
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


_PERIL_ID_TMP      = "peril_id"
_RP_TMP            = "rp"
_VENDOR_AAL_TMP    = "vendor_aal"
_RL_AAL_TMP        = "rl_aal"
_VK_AAL_TMP        = "vk_aal"
_TOTAL_TMP         = "total"
_RL_PROP_TMP       = "rl_proportion"
_VK_PROP_TMP        = "vk_proportion"
_PERIL_NAME_TMP    = "peril_name"

_DEFAULT_EP_TYPES: tuple[str, ...] = (EpType.AAL, EpType.OEP)
_DEFAULT_TARGET_RETURN_PERIODS: tuple[int, ...] = (0, 200, 1000)


log = logging.getLogger("rollup.blending")


def _aal_by_rp_peril(
    long_csvs: list[Path],
    vendor: VendorName,
    analyses: pl.DataFrame,
    ep_types: tuple[str, ...] = _DEFAULT_EP_TYPES,
    target_return_periods: tuple[int, ...] = _DEFAULT_TARGET_RETURN_PERIODS,
) -> pl.DataFrame:
    """Read every long-format EP CSV for a vendor, sum gl per (rp, peril_id).

    Returns DataFrame[rp: Int64, peril_id: Int64, vendor_aal: Float64].
    Empty `long_csvs` (vendor not yet delivered) returns a 0-row frame.
    """
    if not long_csvs:
        log.warning(f"{vendor.value}: no EP long CSVs found — vendor AAL will be 0")
        return pl.DataFrame(schema={_RP_TMP: pl.Int64, _PERIL_ID_TMP: pl.Int64, _VENDOR_AAL_TMP: pl.Float64})

    ep = pl.concat([pl.scan_csv(p) for p in long_csvs], how="vertical_relaxed").collect()

    if vendor == VendorName.RISKLINK:
        peril_label_col = RL.REGION_PERIL
    elif vendor == VendorName.VERISK:
        peril_label_col = VK.ANALYSIS if VK.ANALYSIS in ep.columns else "analysis"
    else:
        raise ValueError(f"unknown vendor {vendor!r}")

    ep_filter = ep.filter(
        pl.col(RL.EP_TYPE).is_in(ep_types)
        & pl.col(RL.RP).is_in(target_return_periods)
    )

    label_to_pid = (
        analyses
        .filter(pl.col(AN.VENDOR) == vendor.value)
        .select(
            pl.col(AN.MODELLED_LABEL).alias(peril_label_col),
            pl.col(AN.PERIL_ID),
        )
        .unique()
    )

    joined = ep_filter.join(label_to_pid, on=peril_label_col, how="left")

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
        .group_by(RL.RP, AN.PERIL_ID)
        .agg(pl.col(RL.GL).sum().alias(_VENDOR_AAL_TMP))
        .rename({RL.RP: _RP_TMP, AN.PERIL_ID: _PERIL_ID_TMP})
    )


def derive_blending_weights(
    rl_long_csvs: list[Path],
    vk_long_csvs: list[Path],
    analyses: pl.DataFrame,
    perils: pl.DataFrame,
    *,
    ep_types: tuple[str, ...] = _DEFAULT_EP_TYPES,
    target_return_periods: tuple[int, ...] = _DEFAULT_TARGET_RETURN_PERIODS,
) -> pl.DataFrame:
    """Compute long-format blending_weights from EP summaries per return period.

    Returns one row per (peril_id, return_period, vendor); peril_name and
    description are populated from `perils`. `sub_peril` is None.
    `weight` is the vendor proportion for that return-period bucket. The
    default buckets are 0=AAL, 200=1-in-200 OEP, and 1000=1-in-1000 OEP.
    `base_model` is populated for operator review/override in the seed.
    """
    rl_aal = _aal_by_rp_peril(
        rl_long_csvs, VendorName.RISKLINK, analyses, ep_types, target_return_periods
    )
    vk_aal = _aal_by_rp_peril(
        vk_long_csvs, VendorName.VERISK, analyses, ep_types, target_return_periods
    )

    rl_aal = rl_aal.rename({_VENDOR_AAL_TMP: _RL_AAL_TMP})
    vk_aal = vk_aal.rename({_VENDOR_AAL_TMP: _VK_AAL_TMP})

    joined = rl_aal.join(vk_aal, on=[_RP_TMP, _PERIL_ID_TMP], how="full", coalesce=True)

    from rollup.config import FLOOD_FAMILY

    proportions = (
        joined
        .with_columns(
            pl.col(_RL_AAL_TMP).fill_null(0.0),
            pl.col(_VK_AAL_TMP).fill_null(0.0),
        )
        .with_columns(**{_TOTAL_TMP: pl.col(_RL_AAL_TMP) + pl.col(_VK_AAL_TMP)})
        .with_columns(
            **{_RL_PROP_TMP: pl.when(pl.col(_TOTAL_TMP) > 0)
               .then(pl.col(_RL_AAL_TMP) / pl.col(_TOTAL_TMP))
               .otherwise(pl.lit(0.5))},
        )
        .with_columns(**{_VK_PROP_TMP: pl.lit(1.0) - pl.col(_RL_PROP_TMP)})
        .join(
            perils.select(P.PERIL_ID, P.NAME, P.PERIL_FAMILY).rename(
                {P.PERIL_ID: _PERIL_ID_TMP, P.NAME: _PERIL_NAME_TMP}
            ),
            on=_PERIL_ID_TMP,
            how="left",
        )
        .with_columns(
            pl.when(pl.col(P.PERIL_FAMILY) == FLOOD_FAMILY)
            .then(pl.lit(VendorName.RISKLINK.value))
            .otherwise(pl.lit(VendorName.VERISK.value))
            .alias(BW.BASE_MODEL),
        )
    )

    rl_rows = proportions.select(
        pl.col(_PERIL_ID_TMP).alias(BW.PERIL_ID),
        pl.col(_RP_TMP).alias(BW.RETURN_PERIOD),
        pl.col(_PERIL_NAME_TMP).alias(BW.PERIL_NAME),
        pl.lit("derived from EP-summary AAL/OEP buckets").alias(BW.DESCRIPTION),
        pl.lit(None, dtype=pl.String).alias(BW.SUB_PERIL),
        pl.lit(VendorName.RISKLINK.value).alias(BW.VENDOR),
        pl.col(BW.BASE_MODEL),
        pl.col(_RL_PROP_TMP).alias(BW.WEIGHT),
    )
    vk_rows = proportions.select(
        pl.col(_PERIL_ID_TMP).alias(BW.PERIL_ID),
        pl.col(_RP_TMP).alias(BW.RETURN_PERIOD),
        pl.col(_PERIL_NAME_TMP).alias(BW.PERIL_NAME),
        pl.lit("derived from EP-summary AAL/OEP buckets").alias(BW.DESCRIPTION),
        pl.lit(None, dtype=pl.String).alias(BW.SUB_PERIL),
        pl.lit(VendorName.VERISK.value).alias(BW.VENDOR),
        pl.col(BW.BASE_MODEL),
        pl.col(_VK_PROP_TMP).alias(BW.WEIGHT),
    )
    out = pl.concat([rl_rows, vk_rows]).sort([BW.PERIL_ID, BW.RETURN_PERIOD, BW.VENDOR])
    if out.height == 0:
        raise ValueError(
            "derive_blending_weights produced 0 rows — every EP-summary "
            "label was unmapped. Check `analyses.modelled_label` covers "
            "the labels seen in the long CSVs."
        )
    return out
