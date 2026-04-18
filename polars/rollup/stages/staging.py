"""Staging: raw vendor YLTs → canonical NormalizedYlt.

RiskLink implementation is representative. Verisk mirrors the shape but
joins `Analysis` (on the raw) → `dim_region_perils.modelled_region_peril`
and filters `CatalogTypeCode='STC'` (per duckdb `int_vw_vk_ylt`).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from rollup.schemas import frames as F
from rollup.schemas.columns import DimRegionPerilCol as DP
from rollup.schemas.columns import DimRisklinkAnalysisCol as DRA
from rollup.schemas.columns import NormalizedYltCol as Y
from rollup.schemas.columns import RawRisklinkYltCol as RLK
from rollup.schemas.columns import RefLobsCol as LB
from rollup.validate import validate_schema


# --------------------------------------------------------------------------- #
# Loaders                                                                     #
# --------------------------------------------------------------------------- #

def load_raw_risklink_ylt(
    parquet_dir: Path,
    *,
    glob: str = "risklink_ylt_*.parquet",
) -> pl.LazyFrame:
    """Scan every RiskLink YLT parquet under `parquet_dir` matching `glob`.

    Multiple files are concatenated lazily — `scan_parquet` with a glob
    never materializes intermediate frames.
    """
    lf = pl.scan_parquet(str(parquet_dir / glob))
    validate_schema(lf, F.RAW_RISKLINK_YLT, name="raw_risklink_ylt", strict=False)
    return lf


def load_raw_verisk_ylt(
    parquet_dir: Path,
    *,
    glob: str = "air_ylt_*.parquet",
) -> pl.LazyFrame:
    """Scan every Verisk (AIR) YLT parquet under `parquet_dir` matching `glob`.

    Verisk ships the YLT in N chunks (e.g. `air_ylt_c1.parquet`,
    `air_ylt_c2.parquet`); the glob reads all chunks as a single lazy table.
    Validates the wire schema (strict=False so extra columns like `filename`
    pass through unchanged).
    """
    lf = pl.scan_parquet(str(parquet_dir / glob))
    validate_schema(lf, F.RAW_VERISK_YLT, name="raw_verisk_ylt", strict=False)
    return lf


# --------------------------------------------------------------------------- #
# Normalizers                                                                 #
# --------------------------------------------------------------------------- #

def normalize_risklink_ylt(
    raw: pl.LazyFrame,
    dim_risklink_analysis: pl.LazyFrame,
    dim_region_perils: pl.LazyFrame,
    lobs: pl.LazyFrame,
) -> pl.LazyFrame:
    """RiskLink YLT → NormalizedYlt. Mirrors duckdb `int_vw_rl_ylt`."""
    validate_schema(raw,                   F.RAW_RISKLINK_YLT,     name="risklink.raw", strict=False)
    validate_schema(dim_risklink_analysis, F.DIM_RISKLINK_ANALYSIS, name="risklink.dim_risklink_analysis")
    validate_schema(dim_region_perils,     F.DIM_REGION_PERILS,     name="risklink.dim_region_perils")
    validate_schema(lobs,                  F.REF_LOBS,              name="risklink.lobs")

    # Pre-select right-side dims and rename their primary keys to their final
    # canonical names. This both drops ambiguous columns (both dim_region_perils
    # AND lobs have a column called "id") and removes the final-select rename step.
    region_perils = dim_region_perils.select(
        pl.col(DP.ID).alias(Y.REGION_PERIL_ID),
        pl.col(DP.MODELLED_REGION_PERIL),
        pl.col(DP.ROLLUP_REGION_PERIL),
    )
    lob_dim = lobs.select(
        pl.col(LB.LOB_ID),
        pl.col(LB.MODELLED_LOB),
        pl.col(LB.ROLLUP_LOB),
        pl.col(LB.LOB_TYPE),
        pl.col(LB.CDS_CAT_CLASS_NAME),
    )

    out = (
        raw
        .join(dim_risklink_analysis, left_on=RLK.ANLS_ID,       right_on=DRA.RISKLINK_ANALYSIS_ID, how="inner")
        .join(region_perils,         left_on=DRA.REGION_PERIL,  right_on=DP.MODELLED_REGION_PERIL, how="inner")
        .join(lob_dim,               left_on=DRA.LOB,           right_on=LB.MODELLED_LOB,           how="inner")
        .select(
            pl.lit("risklink").alias(Y.VENDOR),
            pl.col(Y.LOB_ID),
            pl.col(DRA.LOB).alias(Y.MODELLED_LOB),
            pl.col(Y.ROLLUP_LOB),
            pl.col(Y.LOB_TYPE),
            pl.col(Y.CDS_CAT_CLASS_NAME),
            pl.col(Y.REGION_PERIL_ID),
            pl.col(DRA.REGION_PERIL).alias(Y.MODELLED_REGION_PERIL),
            pl.col(Y.ROLLUP_REGION_PERIL),
            pl.lit(0, dtype=pl.Int64).alias(Y.MODEL_CODE),
            pl.col(RLK.YEAR_ID).alias(Y.YEAR_ID),
            pl.col(RLK.EVENT_ID).alias(Y.EVENT_ID),
            pl.col(RLK.LOSS).alias(Y.LOSS),
        )
    )

    validate_schema(out, F.NORMALIZED_YLT, name="risklink.normalized")
    return out
