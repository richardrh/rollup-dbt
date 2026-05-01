"""Tests for rollup.stages.blending — derive_blending_weights and helpers."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl
import pytest

from rollup.stages.blending import derive_blending_weights, _aal_by_peril
from rollup.config import VendorName
from rollup.schemas.columns import (
    AnalysesCol as AN,
    BlendingWeightsCol as BW,
    PerilsCol as P,
    StgRisklinkEpCol as RL,
)


# ---------------------------------------------------------------------------
# Tiny fixtures shared across tests
# ---------------------------------------------------------------------------

def _make_analyses() -> pl.DataFrame:
    """Minimal analyses table: two risklink perils (1, 2) and one verisk peril (1)."""
    return pl.DataFrame(
        {
            AN.VENDOR:         ["risklink", "risklink", "verisk"],
            AN.ANALYSIS_ID:    ["1",        "2",        "EU_EQ"],
            AN.MODELLED_LABEL: ["EU EQ",    "EU FL",    "EU_EQ"],
            AN.PERIL_ID:       [1,          2,          1],
            AN.LOB_ID:         [10,         20,         None],
        },
        schema={
            AN.VENDOR:         pl.String,
            AN.ANALYSIS_ID:    pl.String,
            AN.MODELLED_LABEL: pl.String,
            AN.PERIL_ID:       pl.Int64,
            AN.LOB_ID:         pl.Int64,
        },
    )


def _make_perils() -> pl.DataFrame:
    return pl.DataFrame(
        {
            P.PERIL_ID:     [1, 2],
            P.NAME:         ["Europe EQ", "Europe FL"],
            P.REGION:       ["EU",        "EU"],
            P.PERIL_FAMILY: ["EQ",        "FL"],
        },
        schema={
            P.PERIL_ID:     pl.Int64,
            P.NAME:         pl.String,
            P.REGION:       pl.String,
            P.PERIL_FAMILY: pl.String,
        },
    )


def _write_rl_long_csv(tmp_path: Path, rows: list[dict]) -> Path:
    """Write a risklink-shaped long CSV to tmp_path."""
    df = pl.DataFrame(
        rows,
        schema={
            RL.ID:           pl.Int64,
            RL.RP:           pl.Int64,
            RL.EP_TYPE:      pl.String,
            RL.LOB:          pl.String,
            RL.REGION_PERIL: pl.String,
            RL.GL:           pl.Float64,
        },
    )
    path = tmp_path / "rms_ep_summary.long.csv"
    df.write_csv(path)
    return path


# ---------------------------------------------------------------------------
# test_derive_blending_uses_aal_only
# ---------------------------------------------------------------------------

def test_derive_blending_uses_aal_only(tmp_path: Path):
    """OEP rows must NOT contribute to the AAL sums."""
    rows = [
        # AAL row for peril 1 (EU EQ) — should count
        {RL.ID: 1, RL.RP: 0,   RL.EP_TYPE: "AAL", RL.LOB: "MGA", RL.REGION_PERIL: "EU EQ", RL.GL: 1000.0},
        # OEP row for peril 1 — must NOT count
        {RL.ID: 1, RL.RP: 100, RL.EP_TYPE: "OEP", RL.LOB: "MGA", RL.REGION_PERIL: "EU EQ", RL.GL: 5000.0},
        # AEP row for peril 1 — must NOT count
        {RL.ID: 1, RL.RP: 200, RL.EP_TYPE: "AEP", RL.LOB: "MGA", RL.REGION_PERIL: "EU EQ", RL.GL: 9000.0},
    ]
    rl_csv = _write_rl_long_csv(tmp_path, rows)
    vk_csv = tmp_path / "verisk_ep_summary.long.csv"   # intentionally absent

    analyses = _make_analyses()
    perils   = _make_perils()

    df = derive_blending_weights(
        [rl_csv],
        [vk_csv] if vk_csv.exists() else [],
        analyses,
        perils,
    )

    # Only peril 1 should be present (peril 2 has no data).
    rl_row = df.filter(
        (pl.col(BW.PERIL_ID) == 1) & (pl.col(BW.VENDOR) == VendorName.RISKLINK)
    )
    assert rl_row.height == 1
    # rl_aal = 1000.0; vk_aal = 0.0 (no verisk CSV) → rl_proportion = 1.0
    assert rl_row[BW.WEIGHT][0] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# test_derive_blending_proportions_sum_to_1
# ---------------------------------------------------------------------------

def test_derive_blending_proportions_sum_to_1(tmp_path: Path):
    """For each peril, rl_weight + vk_weight == 1.0 (within float tolerance)."""
    rows = [
        {RL.ID: 1, RL.RP: 0, RL.EP_TYPE: "AAL", RL.LOB: "MGA", RL.REGION_PERIL: "EU EQ", RL.GL: 300.0},
        {RL.ID: 2, RL.RP: 0, RL.EP_TYPE: "AAL", RL.LOB: "MGA", RL.REGION_PERIL: "EU FL", RL.GL: 700.0},
    ]
    rl_csv = _write_rl_long_csv(tmp_path, rows)
    vk_csv = tmp_path / "verisk_ep_summary.long.csv"  # absent → vk_aal = 0

    df = derive_blending_weights(
        [rl_csv],
        [vk_csv] if vk_csv.exists() else [],
        _make_analyses(),
        _make_perils(),
    )

    for peril_id in df[BW.PERIL_ID].unique().to_list():
        subset = df.filter(pl.col(BW.PERIL_ID) == peril_id)
        total = subset[BW.WEIGHT].sum()
        assert total == pytest.approx(1.0), (
            f"peril {peril_id}: rl+vk weights = {total}, expected 1.0"
        )


# ---------------------------------------------------------------------------
# test_derive_blending_handles_missing_vendor
# ---------------------------------------------------------------------------

def test_derive_blending_handles_missing_vendor(tmp_path: Path):
    """Pass vk_long_csv pointing to a non-existent file.

    The function must still return a DataFrame, and for any peril where
    risklink has AAL data and verisk has none, rl_proportion must equal 1.0.
    """
    rows = [
        {RL.ID: 1, RL.RP: 0, RL.EP_TYPE: "AAL", RL.LOB: "MGA", RL.REGION_PERIL: "EU EQ", RL.GL: 500.0},
    ]
    rl_csv = _write_rl_long_csv(tmp_path, rows)
    vk_csv = Path("/does/not/exist/verisk_ep_summary.long.csv")

    df = derive_blending_weights(
        [rl_csv],
        [vk_csv] if vk_csv.exists() else [],
        _make_analyses(),
        _make_perils(),
    )

    assert isinstance(df, pl.DataFrame)
    assert df.height > 0

    rl_row = df.filter(
        (pl.col(BW.PERIL_ID) == 1) & (pl.col(BW.VENDOR) == VendorName.RISKLINK)
    )
    assert rl_row.height == 1
    assert rl_row[BW.WEIGHT][0] == pytest.approx(1.0), (
        "expected rl_proportion=1.0 when verisk has no data"
    )

    vk_row = df.filter(
        (pl.col(BW.PERIL_ID) == 1) & (pl.col(BW.VENDOR) == VendorName.VERISK)
    )
    assert vk_row.height == 1
    assert vk_row[BW.WEIGHT][0] == pytest.approx(0.0), (
        "expected vk_proportion=0.0 when verisk has no data"
    )


# ---------------------------------------------------------------------------
# test_derive_blending_warns_on_unmapped_label
# ---------------------------------------------------------------------------

def test_derive_blending_warns_on_unmapped_label(tmp_path: Path, caplog):
    """A long CSV row with an unknown region_peril must be skipped with a warning."""
    rows = [
        # Valid row — maps to peril 1
        {RL.ID: 1, RL.RP: 0, RL.EP_TYPE: "AAL", RL.LOB: "MGA", RL.REGION_PERIL: "EU EQ",        RL.GL: 100.0},
        # Unknown label — should be skipped with a warning
        {RL.ID: 2, RL.RP: 0, RL.EP_TYPE: "AAL", RL.LOB: "MGA", RL.REGION_PERIL: "made-up label", RL.GL: 999.0},
    ]
    rl_csv = _write_rl_long_csv(tmp_path, rows)
    vk_csv = tmp_path / "verisk_ep_summary.long.csv"  # absent

    with caplog.at_level(logging.WARNING, logger="rollup.blending"):
        df = derive_blending_weights(
        [rl_csv],
        [vk_csv] if vk_csv.exists() else [],
        _make_analyses(),
        _make_perils(),
    )

    # The unmapped row must be absent from the output.
    # Only peril 1 should appear.
    assert set(df[BW.PERIL_ID].to_list()) == {1}

    # A warning must have been logged mentioning the bad label.
    warning_text = " ".join(r.message for r in caplog.records if r.levelno >= logging.WARNING)
    assert "made-up label" in warning_text, (
        f"expected warning mentioning 'made-up label'; got: {warning_text!r}"
    )
