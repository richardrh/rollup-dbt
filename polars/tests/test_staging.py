"""Staging: catches the lob_id / region_peril_id collision regression and
verifies the analyses + perils + lobs join chain produces a clean
NormalizedYlt for both vendors."""

from __future__ import annotations

import polars as pl

from rollup.config import VendorName
from rollup.schemas import frames as F
from rollup.schemas.columns import AnalysesCol as AN
from rollup.schemas.columns import NormalizedYltCol as Y
from rollup.schemas.columns import PerilsCol as P
from rollup.schemas.columns import RawRisklinkYltCol as RLK
from rollup.schemas.columns import RawVeriskYltCol as VK
from rollup.schemas.columns import RefLobsCol as LB
from rollup.stages.staging import normalize_risklink_ylt, normalize_verisk_ylt


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
        AN.ANALYSIS_ID:    ["EU_WS",             "501"],
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


# --------------------------------------------------------------------------- #
# apply_rollup_scope                                                          #
# --------------------------------------------------------------------------- #

from rollup.schemas.columns import RollupScopeCol as RS
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.stages.staging import apply_rollup_scope


def _scope(*rows: tuple[str, VendorName, str, bool]) -> pl.LazyFrame:
    return pl.DataFrame({
        RS.MODELLED_LOB: [r[0] for r in rows],
        RS.VENDOR:       [r[1] for r in rows],
        RS.ANALYSIS_ID:  [r[2] for r in rows],
        RS.IN_ROLLUP:    [r[3] for r in rows],
    }, schema={
        RS.MODELLED_LOB: pl.String,
        RS.VENDOR:       pl.String,
        RS.ANALYSIS_ID:  pl.String,
        RS.IN_ROLLUP:    pl.Boolean,
    }).lazy()


def _scoped_ylt(label: str, *, modelled_lob: str = "HIC_HH_UK",
                vendor: VendorName = VendorName.VERISK) -> pl.LazyFrame:
    """Minimal post-staging YLT row with the (modelled_lob, vendor, modelled_label)
    triple that apply_rollup_scope joins on."""
    return pl.DataFrame({
        Y.MODELLED_LOB:          [modelled_lob],
        Y.VENDOR:                [vendor],
        Y.MODELLED_REGION_PERIL: [label],
    }, schema={
        Y.MODELLED_LOB:          pl.String,
        Y.VENDOR:                pl.String,
        Y.MODELLED_REGION_PERIL: pl.String,
    }).lazy()


def test_apply_rollup_scope_keeps_in_scope_rows():
    ylt   = _scoped_ylt("EU_WS", modelled_lob="HIC_HH_UK", vendor=VendorName.VERISK)
    scope = _scope(("HIC_HH_UK", VendorName.VERISK, "EU_WS", True))
    assert apply_rollup_scope(ylt, scope).collect().height == 1


def test_apply_rollup_scope_drops_out_of_scope_rows():
    ylt   = _scoped_ylt("EU_WS", modelled_lob="HIC_HH_UK", vendor=VendorName.VERISK)
    scope = _scope(("HIC_HH_UK", VendorName.VERISK, "EU_WS", False))
    assert apply_rollup_scope(ylt, scope).collect().height == 0


def test_apply_rollup_scope_drops_unlisted_rows():
    """A (modelled_lob, vendor, analysis) triple absent from rollup_scope is dropped."""
    ylt   = _scoped_ylt("EU_FL", modelled_lob="HIC_HH_UK", vendor=VendorName.VERISK)
    scope = _scope(("HIC_HH_UK", VendorName.VERISK, "EU_WS", True))   # only EU_WS in scope
    assert apply_rollup_scope(ylt, scope).collect().height == 0
