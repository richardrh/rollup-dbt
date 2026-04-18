"""Staging: catches the id-collision bug that the pre-select fix resolved."""

from __future__ import annotations

import polars as pl

from rollup.schemas import frames as F
from rollup.schemas.columns import NormalizedYltCol as Y
from rollup.stages.staging import normalize_risklink_ylt


def _raw_risklink_ylt() -> pl.LazyFrame:
    return pl.DataFrame({
        "SimulationSetId": [1],
        "yearid": [2026],
        "eventid": [100],
        "date": ["2026-01-01"],
        "p_value": [0.5],
        "anlsid": [500],                 # → dim_risklink_analysis
        "name": ["x"],
        "description": ["x"],
        "rate": [0.01],
        "meanloss": [1.0],
        "stddev": [0.1],
        "expvalue": [1.0],
        "loss": [123.45],
    }, schema=F.RAW_RISKLINK_YLT).lazy()


def _dim_risklink_analysis() -> pl.LazyFrame:
    return pl.DataFrame({
        "risklink_analysis_id": [500],
        "lob": ["lob_a"],
        "region_peril": ["eu_ws"],
    }, schema=F.DIM_RISKLINK_ANALYSIS).lazy()


def _dim_region_perils() -> pl.LazyFrame:
    """region_peril id=7 — must NOT leak into lob_id after the join."""
    return pl.DataFrame({
        "id": [7],
        "vendor": ["risklink"],
        "modelled_region_peril": ["eu_ws"],
        "cleaned_region_peril": ["eu_ws"],
        "rollup_region_peril": ["EU_WS"],
        "region": ["EU"],
        "peril": ["WS"],
        "adjustments": [""],
        "excludes": [""],
        "applies_to_mga": [1],
        "applies_to_prop": [1],
        "applies_to_fa": [0],
        "blending_factor_region_peril_id": [1],
        "blending_factor_sub_region_peril_id": ["EU_WS"],
    }, schema=F.DIM_REGION_PERILS).lazy()


def _lobs() -> pl.LazyFrame:
    """lobs lob_id=42 — must land in lob_id (NOT the region-peril id=7)."""
    return pl.DataFrame({
        "lob_id": [42],
        "modelled_lob": ["lob_a"],
        "rollup_lob": ["ROLLUP_A"],
        "lob_type": ["prop"],
        "cds_cat_class_name": ["class_a"],
        "office": ["UK"],
        "class": ["HH"],
    }, schema=F.REF_LOBS).lazy()


def test_normalize_risklink_ylt_does_not_confuse_lob_id_with_region_peril_id():
    """Regression: both dim_region_perils AND lobs have a column called 'id'.

    Pre-fix, polars would suffix one of them after the join and the projection
    would silently pick the wrong 'id' for lob_id. Pre-selecting the right-side
    frames with aliased keys eliminates the ambiguity entirely.
    """
    out = normalize_risklink_ylt(
        _raw_risklink_ylt(), _dim_risklink_analysis(), _dim_region_perils(), _lobs()
    ).collect()

    assert out.height == 1
    row = out.row(0, named=True)
    assert row[Y.LOB_ID]          == 42, "lob_id must come from lobs.lob_id, not dim_region_perils.id"
    assert row[Y.REGION_PERIL_ID] == 7,  "region_peril_id must come from dim_region_perils.id"
    assert row[Y.VENDOR]              == "risklink"
    assert row[Y.ROLLUP_LOB]          == "ROLLUP_A"
    assert row[Y.ROLLUP_REGION_PERIL] == "EU_WS"
    assert row[Y.LOSS]                == 123.45


def test_normalize_risklink_ylt_output_matches_schema():
    out = normalize_risklink_ylt(
        _raw_risklink_ylt(), _dim_risklink_analysis(), _dim_region_perils(), _lobs()
    ).collect()
    assert out.schema == F.NORMALIZED_YLT
