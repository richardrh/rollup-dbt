"""Staging: raw vendor YLTs → canonical NormalizedYlt.

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
from rollup.schemas.columns import RollupScopeCol as RS
from rollup.validate import validate_schema


log = logging.getLogger("rollup.staging")


# --------------------------------------------------------------------------- #
# Loaders                                                                     #
# --------------------------------------------------------------------------- #

def load_raw_risklink_ylt(
    parquet_dir: Path,
    *,
    glob: str = "risklink_ylt_*.parquet",
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
    `office` and `lob_class` directly to the NormalizedYlt frame."""
    return lobs.select(
        pl.col(LB.LOB_ID),
        pl.col(LB.MODELLED_LOB),
        pl.col(LB.ROLLUP_LOB),
        pl.col(LB.LOB_TYPE),
        pl.col(LB.CDS_CAT_CLASS_NAME),
        pl.col(LB.OFFICE),
        pl.col(LB.CLASS).alias(Y.LOB_CLASS),
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
    """Vendor-filtered analyses with the modelled label aliased so the join
    against the YLT's analysis-string column lands cleanly."""
    return (
        analyses
        .filter(pl.col(AN.VENDOR) == vendor)
        .select(
            pl.col(AN.ANALYSIS_ID),
            pl.col(AN.MODELLED_LABEL).alias(Y.MODELLED_REGION_PERIL),
            pl.col(AN.PERIL_ID).alias(Y.REGION_PERIL_ID),
            pl.col(AN.LOB_ID),                  # nullable for verisk; populated for risklink
        )
    )


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
        raw.Analysis           → analyses.analysis_id (vendor='verisk') → peril_id
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
        .join(vk_analyses,        left_on=VK.ANALYSIS,           right_on=AN.ANALYSIS_ID, how="inner")
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
            pl.col(VK.MODEL_CODE).alias(Y.MODEL_CODE),
            pl.col(VK.YEAR_ID).alias(Y.YEAR_ID),
            pl.col(VK.EVENT_ID).alias(Y.EVENT_ID),
            pl.col(VK.NET_PRE_CAT_LOSS).alias(Y.LOSS),
        )
    )

    validate_schema(out, F.NORMALIZED_YLT, name="verisk.normalized")
    return out


# --------------------------------------------------------------------------- #
# Scope filter — drops YLT rows not officially in the rollup                  #
# --------------------------------------------------------------------------- #

def apply_rollup_scope(ylt: pl.LazyFrame, rollup_scope: pl.LazyFrame) -> pl.LazyFrame:
    """Inner-join `rollup_scope` to keep only (modelled_lob, vendor, analysis_id)
    triples whose `in_rollup` is True. Lives in staging because it is a
    gate, not a factor — produces no new column, only filters rows.

    `analysis_id` in `rollup_scope` is the **modelled label** the YLT carries
    after staging (`MODELLED_REGION_PERIL`), not the raw RiskLink integer id.

    If `rollup_scope` is empty the inner join drops every row — by design.
    The pre-flight `build_plan` reporter flags an empty `rollup_scope` as a
    blocker so the run aborts before producing zero-row Hisco parquets.
    """
    in_scope = (
        rollup_scope
        .filter(pl.col(RS.IN_ROLLUP))
        .select(
            pl.col(RS.MODELLED_LOB),
            pl.col(RS.VENDOR),
            pl.col(RS.ANALYSIS_ID).alias(Y.MODELLED_REGION_PERIL),
        )
    )
    # Keyed on (modelled_lob, vendor, analysis_label) — readable without a join
    # to lobs.csv. Two analyses can share a peril_id (e.g. UK_WSSS and
    # UK_WSSS_GCAdj are both peril 206 but only one is official per LOB).
    out = ylt.join(
        in_scope,
        left_on=[Y.MODELLED_LOB, Y.VENDOR, Y.MODELLED_REGION_PERIL],
        right_on=[RS.MODELLED_LOB, RS.VENDOR, Y.MODELLED_REGION_PERIL],
        how="inner",
    )
    log.info("rollup_scope: filtered YLT to in_rollup=True triples")
    return out
