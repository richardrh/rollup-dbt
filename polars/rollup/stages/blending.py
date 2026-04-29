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
    PerilsCol as P,
    StgRisklinkEpCol as RL,
    StgVeriskEpCol as VK,
)


log = logging.getLogger("rollup.blending")


def _aal_by_peril(
    long_csv: Path,
    vendor: VendorName,
    analyses: pl.DataFrame,
) -> pl.DataFrame:
    """Read a long-format EP CSV, filter to AAL rows, map region_peril to peril_id, sum AAL per peril.

    Returns DataFrame[peril_id: Int64, vendor_aal: Float64].
    Missing region_peril mappings are warned and skipped.
    """
    if not long_csv.exists():
        log.warning(
            f"{vendor.value}: EP long CSV not found at {long_csv} "
            "— vendor AAL will be 0"
        )
        return pl.DataFrame(schema={"peril_id": pl.Int64, "vendor_aal": pl.Float64})

    ep = pl.read_csv(long_csv)

    if vendor == VendorName.RISKLINK:
        peril_label_col = RL.REGION_PERIL
    elif vendor == VendorName.VERISK:
        # Verisk uses 'analysis' instead of 'region_peril' as the peril label.
        peril_label_col = VK.ANALYSIS if VK.ANALYSIS in ep.columns else "analysis"
    else:
        raise ValueError(f"unknown vendor {vendor!r}")

    aal_rows = ep.filter(pl.col("ep_type") == "AAL")

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
        bad_labels = sorted(set(unmapped[peril_label_col].to_list()))
        log.warning(
            f"{vendor.value}: {len(bad_labels)} EP-summary labels not in "
            f"analyses.csv: {bad_labels}"
        )

    return (
        joined
        .filter(pl.col(AN.PERIL_ID).is_not_null())
        .group_by(AN.PERIL_ID)
        .agg(pl.col("gl").sum().alias("vendor_aal"))
        .rename({AN.PERIL_ID: "peril_id"})
    )


def derive_blending_weights(
    rl_long_csv: Path,
    vk_long_csv: Path,
    analyses: pl.DataFrame,
    perils: pl.DataFrame,
) -> pl.DataFrame:
    """Compute long-format blending_weights from EP-summary AALs.

    Returns one row per (peril_id, vendor); peril_name and description are
    populated from `perils`. `sub_peril` is None. `weight` is the proportion.
    """
    rl_aal = _aal_by_peril(rl_long_csv, VendorName.RISKLINK, analyses)
    vk_aal = _aal_by_peril(vk_long_csv, VendorName.VERISK, analyses)

    rl_aal = rl_aal.rename({"vendor_aal": "rl_aal"})
    vk_aal = vk_aal.rename({"vendor_aal": "vk_aal"})

    # Outer join so any peril seen by either vendor is represented.
    joined = rl_aal.join(vk_aal, on="peril_id", how="full", coalesce=True)

    proportions = (
        joined
        .with_columns(
            pl.col("rl_aal").fill_null(0.0),
            pl.col("vk_aal").fill_null(0.0),
        )
        .with_columns(
            total=(pl.col("rl_aal") + pl.col("vk_aal")),
        )
        .with_columns(
            rl_proportion=pl.when(pl.col("total") > 0)
            .then(pl.col("rl_aal") / pl.col("total"))
            .otherwise(pl.lit(0.5)),
        )
        .with_columns(vk_proportion=pl.lit(1.0) - pl.col("rl_proportion"))
        .join(
            perils.select(P.PERIL_ID, P.NAME).rename(
                {P.PERIL_ID: "peril_id", P.NAME: "peril_name"}
            ),
            on="peril_id",
            how="left",
        )
    )

    # Long-format: one row per (peril_id, vendor).
    rl_rows = proportions.select(
        pl.col("peril_id").alias(BW.PERIL_ID),
        pl.col("peril_name").alias(BW.PERIL_NAME),
        pl.lit("derived from EP-summary AALs").alias(BW.DESCRIPTION),
        pl.lit(None, dtype=pl.String).alias(BW.SUB_PERIL),
        pl.lit(VendorName.RISKLINK.value).alias(BW.VENDOR),
        pl.col("rl_proportion").alias(BW.WEIGHT),
    )
    vk_rows = proportions.select(
        pl.col("peril_id").alias(BW.PERIL_ID),
        pl.col("peril_name").alias(BW.PERIL_NAME),
        pl.lit("derived from EP-summary AALs").alias(BW.DESCRIPTION),
        pl.lit(None, dtype=pl.String).alias(BW.SUB_PERIL),
        pl.lit(VendorName.VERISK.value).alias(BW.VENDOR),
        pl.col("vk_proportion").alias(BW.WEIGHT),
    )
    return pl.concat([rl_rows, vk_rows]).sort([BW.PERIL_ID, BW.VENDOR])
