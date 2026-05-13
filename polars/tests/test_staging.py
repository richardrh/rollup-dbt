"""Staging: catches the lob_id / region_peril_id collision regression and
verifies the analyses + perils + lobs join chain produces a clean
NormalizedYlt for both vendors."""

from __future__ import annotations

import polars as pl
import pytest

from rollup.config import VendorName
from rollup.schemas import frames as F
from rollup.schemas.columns import AnalysesCol as AN
from rollup.schemas.columns import NormalizedYltCol as Y
from rollup.schemas.columns import PerilsCol as P
from rollup.schemas.columns import RawRisklinkYltCol as RLK
from rollup.schemas.columns import RawVeriskYltCol as VK
from rollup.schemas.columns import RefLobsCol as LB
from rollup.schemas.columns import ValidAnalysesCol as VA
from rollup.stages.staging import (
    filter_valid_analyses,
    normalize_risklink_ylt,
    normalize_verisk_ylt,
    validate_one_peril_per_rollup_lob,
)


# --------------------------------------------------------------------------- #
# Fixtures                                                                    #
# --------------------------------------------------------------------------- #

def _raw_risklink_ylt() -> pl.LazyFrame:
    return pl.DataFrame({
        RLK.SIMULATION_SET_ID: [1],
        RLK.YEAR_ID:           [2026],
        RLK.EVENT_ID:          [100],
        RLK.DATE:              ["2026-01-01"],
        RLK.P_VALUE:           [0.5],
        RLK.ANLS_ID:           [501],
        RLK.NAME:              ["x"],
        RLK.DESCRIPTION:       ["x"],
        RLK.RATE:              [0.01],
        RLK.MEAN_LOSS:         [1.0],
        RLK.STD_DEV:           [0.1],
        RLK.EXP_VALUE:         [1.0],
        RLK.LOSS:              [123.45],
    }, schema=F.RAW_RISKLINK_YLT).lazy()


def _raw_verisk_ylt() -> pl.LazyFrame:
    return pl.DataFrame({
        VK.ANALYSIS:           ["EU_WS"],
        VK.EXPOSURE_ATTRIBUTE: ["lob_a"],
        VK.CATALOG_TYPE_CODE:  ["STC"],
        VK.EVENT_ID:           [200],
        VK.MODEL_CODE:         [41],
        VK.YEAR_ID:            [2026],
        VK.PERILSET_CODE:      [1],
        VK.GROUND_UP_LOSS:     [120.0],
        VK.GROSS_LOSS:         [110.0],
        VK.NET_PRE_CAT_LOSS:   [100.0],
        VK.FILENAME:           ["test"],
    }, schema=F.RAW_VERISK_YLT).lazy()


def _analyses() -> pl.LazyFrame:
    """One verisk + one risklink analysis pointing at peril 206 (EU_WS)."""
    return pl.DataFrame({
        AN.VENDOR:         [VendorName.VERISK,   VendorName.RISKLINK],
        AN.ANALYSIS_ID:    ["900003",            "501"],
        AN.MODELLED_LABEL: ["EU_WS",             "EU_WS"],
        AN.PERIL_ID:       [206,                 206],
        AN.LOB_ID:         [None,                42],   # NULL for verisk; populated for risklink
    }, schema=F.ANALYSES).lazy()


def _perils() -> pl.LazyFrame:
    return pl.DataFrame({
        P.PERIL_ID:     [206],
        P.NAME:         ["Europe Winter Storm"],
        P.REGION:       ["EU"],
        P.PERIL_FAMILY: ["WS"],
    }, schema=F.PERILS).lazy()


def _lobs() -> pl.LazyFrame:
    return pl.DataFrame({
        LB.LOB_ID:             [42],
        LB.MODELLED_LOB:       ["lob_a"],
        LB.ROLLUP_LOB:         ["ROLLUP_A"],
        LB.LOB_TYPE:           ["prop"],
        LB.CDS_CAT_CLASS_NAME: ["class_a"],
        LB.OFFICE:             ["UK"],
        LB.CLASS:              ["HH"],
    }, schema=F.REF_LOBS).lazy()


def _valid_analyses(*rows: tuple[VendorName, str]) -> pl.LazyFrame:
    return pl.DataFrame({
        VA.VENDOR: [r[0] for r in rows],
        VA.ANALYSIS_ID: [r[1] for r in rows],
    }, schema=F.VALID_ANALYSES).lazy()


# --------------------------------------------------------------------------- #
# Tests                                                                       #
# --------------------------------------------------------------------------- #

def test_normalize_risklink_ylt_resolves_lob_via_analyses():
    """RiskLink: lob_id is carried on the analyses row (not joined separately)."""
    out = normalize_risklink_ylt(
        _raw_risklink_ylt(), _analyses(), _perils(), _lobs(),
    ).collect()

    assert out.height == 1
    row = out.row(0, named=True)
    assert row[Y.LOB_ID]                == 42
    assert row[Y.REGION_PERIL_ID]       == 206
    assert row[Y.VENDOR]                == VendorName.RISKLINK
    assert row[Y.ROLLUP_LOB]            == "ROLLUP_A"
    assert row[Y.PERIL_NAME]            == "Europe Winter Storm"
    assert row[Y.REGION]                == "EU"
    assert row[Y.PERIL_FAMILY]          == "WS"
    assert row[Y.MODELLED_REGION_PERIL] == "EU_WS"
    assert row[Y.LOSS]                  == 123.45


def test_normalize_verisk_ylt_resolves_lob_via_exposure_attribute():
    """Verisk: lob_id comes from the lobs join via ExposureAttribute (analyses.lob_id is null)."""
    out = normalize_verisk_ylt(
        _raw_verisk_ylt(), _analyses(), _perils(), _lobs(),
    ).collect()

    assert out.height == 1
    row = out.row(0, named=True)
    assert row[Y.LOB_ID]                == 42
    assert row[Y.REGION_PERIL_ID]       == 206
    assert row[Y.VENDOR]                == VendorName.VERISK
    assert row[Y.ROLLUP_LOB]            == "ROLLUP_A"
    assert row[Y.PERIL_NAME]            == "Europe Winter Storm"
    assert row[Y.PERIL_FAMILY]          == "WS"
    assert row[Y.MODELLED_REGION_PERIL] == "EU_WS"
    assert row[Y.LOSS]                  == 100.0


def test_normalize_verisk_ylt_matches_trim_upper_stc_contains_filter():
    """January kept rows where trim(upper(CatalogTypeCode)) LIKE '%STC%'."""
    raw = pl.DataFrame({
        VK.ANALYSIS:           ["EU_WS", "EU_WS", "EU_WS"],
        VK.EXPOSURE_ATTRIBUTE: ["lob_a", "lob_a", "lob_a"],
        VK.CATALOG_TYPE_CODE:  [" stc ", "XSTCY", "HIST"],
        VK.EVENT_ID:           [200, 201, 202],
        VK.MODEL_CODE:         [41, 41, 41],
        VK.YEAR_ID:            [2026, 2026, 2026],
        VK.PERILSET_CODE:      [1, 1, 1],
        VK.GROUND_UP_LOSS:     [120.0, 121.0, 122.0],
        VK.GROSS_LOSS:         [110.0, 111.0, 112.0],
        VK.NET_PRE_CAT_LOSS:   [100.0, 101.0, 102.0],
        VK.FILENAME:           ["test", "test", "test"],
    }, schema=F.RAW_VERISK_YLT).lazy()

    out = normalize_verisk_ylt(raw, _analyses(), _perils(), _lobs()).collect()

    assert out[Y.EVENT_ID].sort().to_list() == [200, 201]


def test_normalized_outputs_match_schema():
    rl = normalize_risklink_ylt(_raw_risklink_ylt(), _analyses(), _perils(), _lobs()).collect()
    vk = normalize_verisk_ylt  (_raw_verisk_ylt(),   _analyses(), _perils(), _lobs()).collect()
    assert rl.schema == F.NORMALIZED_YLT
    assert vk.schema == F.NORMALIZED_YLT


def test_filter_valid_analyses_keeps_only_numeric_vendor_ids():
    filtered = filter_valid_analyses(
        _analyses(),
        _valid_analyses((VendorName.VERISK, "900003"), (VendorName.RISKLINK, "501")),
    ).collect()

    assert filtered.select(AN.VENDOR, AN.ANALYSIS_ID).sort(AN.VENDOR).rows() == [
        (VendorName.RISKLINK.value, "501"),
        (VendorName.VERISK.value, "900003"),
    ]


def test_numeric_verisk_valid_analysis_still_joins_raw_label():
    filtered = filter_valid_analyses(
        _analyses(),
        _valid_analyses((VendorName.VERISK, "900003")),
    )

    out = normalize_verisk_ylt(_raw_verisk_ylt(), filtered, _perils(), _lobs()).collect()

    assert out.height == 1
    assert out[Y.MODELLED_REGION_PERIL].to_list() == ["EU_WS"]


def test_verisk_text_label_is_not_a_valid_analysis_id():
    filtered = filter_valid_analyses(
        _analyses(),
        _valid_analyses((VendorName.VERISK, "EU_WS")),
    )

    out = normalize_verisk_ylt(_raw_verisk_ylt(), filtered, _perils(), _lobs()).collect()

    assert out.height == 0


def test_valid_analysis_filtered_metadata_drops_ylt_rows():
    filtered = filter_valid_analyses(
        _analyses(),
        _valid_analyses((VendorName.VERISK, "DOES_NOT_MATCH")),
    )

    out = normalize_verisk_ylt(_raw_verisk_ylt(), filtered, _perils(), _lobs()).collect()

    assert out.height == 0


def test_validate_one_peril_per_rollup_lob_accepts_single_peril():
    ylt = pl.DataFrame({
        Y.ROLLUP_LOB: ["LOB_A", "LOB_A"],
        Y.REGION_PERIL_ID: [1, 1],
    }).lazy()

    validate_one_peril_per_rollup_lob(ylt)


def test_validate_one_peril_per_rollup_lob_rejects_multiple_perils():
    ylt = pl.DataFrame({
        Y.ROLLUP_LOB: ["LOB_A", "LOB_A"],
        Y.REGION_PERIL_ID: [1, 2],
    }).lazy()

    with pytest.raises(ValueError, match="one peril per rollup_lob"):
        validate_one_peril_per_rollup_lob(ylt)
