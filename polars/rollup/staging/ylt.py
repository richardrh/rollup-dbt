"""Staging model: raw vendor YLTs → canonical NormalizedYlt.

Joins for both vendors go through `analyses` (vendor + analysis_id → peril_id
[+ lob_id for RiskLink]) and `perils` (peril_id → name + region + family).
The legacy `dim_region_perils` / `dim_risklink_analysis` god-tables are gone.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from rollup.config import VendorName
from rollup.schemas import frames as F
from rollup.schemas.columns import AnalysesCol as AN
from rollup.schemas.columns import NormalizedYltCol as Y
from rollup.schemas.columns import PerilsCol as P
from rollup.schemas.columns import RawRisklinkYltCol as RLK
from rollup.schemas.columns import RawVeriskYltCol as VK
from rollup.schemas.columns import RefLobsCol as LB
from rollup.schemas.columns import ValidAnalysesCol as VA
from rollup.validate import validate_schema


log = logging.getLogger("rollup.staging")


# --------------------------------------------------------------------------- #
# Loaders                                                                     #
# --------------------------------------------------------------------------- #

def load_raw_risklink_ylt(
    parquet_dir: Path,
    *,
    glob: str = "risklink_ylt*.parquet",
) -> pl.LazyFrame:
    """Scan every RiskLink YLT parquet under `parquet_dir` matching `glob`."""
    pattern = parquet_dir / glob
    lf = pl.scan_parquet(str(pattern))
    validate_schema(lf, F.RAW_RISKLINK_YLT, name="raw_risklink_ylt", strict=False)
    log.info(f"loaded risklink YLT: {pattern}")
    return lf


def load_raw_verisk_ylt(
    parquet_dir: Path,
    *,
    glob: str = "air_ylt_*.parquet",
) -> pl.LazyFrame:
    """Scan every Verisk (AIR) YLT parquet under `parquet_dir` matching `glob`.

    Verisk ships the YLT in N chunks (e.g. `air_ylt_c1.parquet`,
    `air_ylt_c2.parquet`); the glob reads all chunks as a single lazy table.
    """
    pattern = parquet_dir / glob
    lf = pl.scan_parquet(str(pattern))
    validate_schema(lf, F.RAW_VERISK_YLT, name="raw_verisk_ylt", strict=False)
    log.info(f"loaded verisk YLT: {pattern}")
    return lf


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

def _lob_dim(lobs: pl.LazyFrame) -> pl.LazyFrame:
    """Pre-selected lobs columns + the alias for `class` so the join contributes
    `office`, `lob_class`, and `currency` directly to the NormalizedYlt frame."""
    return lobs.select(
        pl.col(LB.LOB_ID),
        pl.col(LB.MODELLED_LOB),
        pl.col(LB.ROLLUP_LOB),
        pl.col(LB.LOB_TYPE),
        pl.col(LB.CDS_CAT_CLASS_NAME),
        pl.col(LB.OFFICE),
        pl.col(LB.CLASS).alias(Y.LOB_CLASS),
        pl.col(LB.CURRENCY),
    )


def _peril_dim(perils: pl.LazyFrame) -> pl.LazyFrame:
    """Pre-selected perils columns aliased to their NormalizedYlt names so
    the join contributes peril_name, region, and peril_family in one step."""
    return perils.select(
        pl.col(P.PERIL_ID).alias(Y.REGION_PERIL_ID),
        pl.col(P.NAME).alias(Y.PERIL_NAME),
        pl.col(P.REGION),
        pl.col(P.PERIL_FAMILY),
    )


def _analyses_for(vendor: VendorName, analyses: pl.LazyFrame) -> pl.LazyFrame:
    """Vendor-filtered analyses with both numeric ID and modelled label.

    ``analysis_id`` is the operator-facing numeric allow-list key. Verisk raw
    YLTs still carry the modelled analysis label, so Verisk staging joins on
    ``modelled_label`` after ``filter_valid_analyses`` has applied the numeric
    ID gate.
    """
    return (
        analyses
        .filter(pl.col(AN.VENDOR) == vendor)
        .select(
            pl.col(AN.ANALYSIS_ID),
            pl.col(AN.MODELLED_LABEL),
            pl.col(AN.MODELLED_LABEL).alias(Y.MODELLED_REGION_PERIL),
            pl.col(AN.PERIL_ID).alias(Y.REGION_PERIL_ID),
            pl.col(AN.LOB_ID),                  # nullable for verisk; populated for risklink
        )
    )


def filter_valid_analyses(
    analyses: pl.LazyFrame,
    valid_analyses: pl.LazyFrame,
) -> pl.LazyFrame:
    """Keep only analysis metadata explicitly listed in valid_analyses.

    The allow-list is keyed by vendor-native numeric ``analysis_id`` for both
    RiskLink and Verisk. Downstream staging and EP blending then use only these
    valid analysis rows; Verisk raw rows still join by ``modelled_label``.
    """
    valid = valid_analyses.select(
        pl.col(VA.VENDOR),
        pl.col(VA.ANALYSIS_ID),
    ).unique()
    return analyses.join(
        valid,
        left_on=[AN.VENDOR, AN.ANALYSIS_ID],
        right_on=[VA.VENDOR, VA.ANALYSIS_ID],
        how="inner",
    )


def validate_one_peril_per_rollup_lob(ylt: pl.LazyFrame) -> None:
    """Compatibility no-op: multiple perils per rollup LOB are valid.

    The active preflight guard now validates the operator allow-list at the
    analysis grain: at most one valid analysis for each concrete LOB/peril pair.
    A rollup LOB can legitimately include multiple perils.
    """
    _ = ylt
    return


# --------------------------------------------------------------------------- #
# Normalizers                                                                 #
# --------------------------------------------------------------------------- #

def normalize_risklink_ylt(
    raw: pl.LazyFrame,
    analyses: pl.LazyFrame,
    perils: pl.LazyFrame,
    lobs: pl.LazyFrame,
) -> pl.LazyFrame:
    """RiskLink YLT → NormalizedYlt.

    For RiskLink, `analyses.lob_id` is populated — one analysis is 1:1 with
    a (lob, peril). So the join chain is:
        raw.anlsid (Int64) → analyses.analysis_id (String, cast) → peril_id + lob_id
        analyses.peril_id  → perils.peril_id → name + region + peril_family
        analyses.lob_id    → lobs.lob_id → office + lob_class + ...
    """
    rl_analyses = _analyses_for(VendorName.RISKLINK, analyses)

    out = (
        raw
        .with_columns(pl.col(RLK.ANLS_ID).cast(pl.String).alias(AN.ANALYSIS_ID))
        .join(rl_analyses,    on=AN.ANALYSIS_ID,             how="inner")
        .join(_peril_dim(perils), on=Y.REGION_PERIL_ID,      how="inner")
        .join(_lob_dim(lobs), left_on=AN.LOB_ID, right_on=LB.LOB_ID, how="inner")
        .select(
            pl.lit(VendorName.RISKLINK).alias(Y.VENDOR),
            pl.col(AN.LOB_ID).alias(Y.LOB_ID),
            pl.col(Y.MODELLED_LOB),
            pl.col(Y.ROLLUP_LOB),
            pl.col(Y.LOB_TYPE),
            pl.col(Y.CDS_CAT_CLASS_NAME),
            pl.col(Y.OFFICE),
            pl.col(Y.LOB_CLASS),
            pl.col(Y.REGION_PERIL_ID),
            pl.col(Y.MODELLED_REGION_PERIL),
            pl.col(Y.PERIL_NAME),
            pl.col(Y.REGION),
            pl.col(Y.PERIL_FAMILY),
            pl.col(Y.CURRENCY),
            pl.lit(0, dtype=pl.Int64).alias(Y.MODEL_CODE),
            pl.col(RLK.YEAR_ID).alias(Y.YEAR_ID),
            pl.col(RLK.EVENT_ID).alias(Y.EVENT_ID),
            pl.col(RLK.LOSS).alias(Y.LOSS),
        )
    )

    validate_schema(out, F.NORMALIZED_YLT, name="risklink.normalized")
    return out


def normalize_verisk_ylt(
    raw: pl.LazyFrame,
    analyses: pl.LazyFrame,
    perils: pl.LazyFrame,
    lobs: pl.LazyFrame,
) -> pl.LazyFrame:
    """Verisk YLT → NormalizedYlt.

    For Verisk, `analyses.lob_id` is NULL — the analysis is peril-only and
    the LOB lives on the YLT row's `ExposureAttribute`. Join chain:
        raw.Analysis           → analyses.modelled_label (vendor='verisk') → peril_id
        analyses.peril_id      → perils.peril_id → name + region + peril_family
        raw.ExposureAttribute  → lobs.modelled_lob → lob_id + ...
    Filters `trim(upper(CatalogTypeCode)) LIKE '%STC%'` (matches january).
    """
    vk_analyses = (
        _analyses_for(VendorName.VERISK, analyses)
        .drop(AN.LOB_ID)   # always null for verisk; lob comes from the YLT row
    )

    out = (
        raw
        .filter(pl.col(VK.CATALOG_TYPE_CODE).str.strip_chars().str.to_uppercase().str.contains("STC"))
        .join(vk_analyses,        left_on=VK.ANALYSIS,           right_on=AN.MODELLED_LABEL, how="inner")
        .join(_peril_dim(perils),                                on=Y.REGION_PERIL_ID,    how="inner")
        .join(_lob_dim(lobs),     left_on=VK.EXPOSURE_ATTRIBUTE, right_on=LB.MODELLED_LOB, how="inner")
        .select(
            pl.lit(VendorName.VERISK).alias(Y.VENDOR),
            pl.col(LB.LOB_ID).alias(Y.LOB_ID),
            pl.col(VK.EXPOSURE_ATTRIBUTE).alias(Y.MODELLED_LOB),
            pl.col(Y.ROLLUP_LOB),
            pl.col(Y.LOB_TYPE),
            pl.col(Y.CDS_CAT_CLASS_NAME),
            pl.col(Y.OFFICE),
            pl.col(Y.LOB_CLASS),
            pl.col(Y.REGION_PERIL_ID),
            pl.col(Y.MODELLED_REGION_PERIL),
            pl.col(Y.PERIL_NAME),
            pl.col(Y.REGION),
            pl.col(Y.PERIL_FAMILY),
            pl.col(Y.CURRENCY),
            pl.col(VK.MODEL_CODE).alias(Y.MODEL_CODE),
            pl.col(VK.YEAR_ID).alias(Y.YEAR_ID),
            pl.col(VK.EVENT_ID).alias(Y.EVENT_ID),
            pl.col(VK.NET_PRE_CAT_LOSS).alias(Y.LOSS),
        )
    )

    validate_schema(out, F.NORMALIZED_YLT, name="verisk.normalized")
    return out
