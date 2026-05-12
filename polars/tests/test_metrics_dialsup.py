"""Unit tests for the DIALSUP metric transformation."""

from __future__ import annotations

import polars as pl
import pytest

from rollup.chain import forecast_factor_col
from rollup.metrics.dialsup import add_dialsup
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import NormalizedYltCol as Y


def test_add_dialsup_single_column_named_dialsup():
    """add_dialsup adds exactly one column called 'dialsup', not per-tag."""
    tag = "202601"
    ylt = pl.DataFrame({
        Y.LOSS:                    [100.0],
        forecast_factor_col(tag):  [1.05],
        AF.EUWS_FACTOR:            [0.95],
        AF.FA_GROSS_AAL_FACTOR:    [1.10],
    }).lazy()

    out = add_dialsup(ylt, tag).collect()

    assert "dialsup" in out.columns
    assert not any(col.startswith("dialsup_") for col in out.columns)


def test_add_dialsup_matches_january_factor_formula():
    """dialsup == raw loss * forecast * euws * fa_gross, with no FX or uplift."""
    tag = "202601"
    ylt = pl.DataFrame({
        Y.LOSS:                    [1000.0, 500.0, 250.0],
        forecast_factor_col(tag):  [1.05,   1.10,  1.0],
        AF.EUWS_FACTOR:            [0.95,   1.00,  0.8],
        AF.FA_GROSS_AAL_FACTOR:    [1.10,   1.20,  1.0],
        AF.RATE_TO_GBP:            [1.25,   0.88,  2.0],
        AF.UPLIFT_FACTOR_CAPPED:   [9.0,    8.0,   7.0],
    }).lazy()

    out = add_dialsup(ylt, tag).collect()

    assert out["dialsup"][0] == pytest.approx(1000.0 * 1.05 * 0.95 * 1.10)
    assert out["dialsup"][1] == pytest.approx(500.0 * 1.10 * 1.00 * 1.20)
    assert out["dialsup"][2] == pytest.approx(250.0 * 1.00 * 0.80 * 1.00)


def test_dialsup_uses_requested_forecast_tag():
    """The DIALSUP fanout has one metric and uses the selected forecast tag."""
    selected_tag = "202607"
    ylt = pl.DataFrame({
        Y.LOSS:                         [800.0, 200.0],
        forecast_factor_col("202601"): [1.01, 1.02],
        forecast_factor_col(selected_tag): [1.10, 1.20],
        AF.EUWS_FACTOR:                 [0.90, 1.00],
        AF.FA_GROSS_AAL_FACTOR:         [1.00, 1.50],
    }).lazy()

    out = add_dialsup(ylt, selected_tag).collect()
    expected = [800.0 * 1.10 * 0.90 * 1.00, 200.0 * 1.20 * 1.00 * 1.50]

    for i, exp in enumerate(expected):
        assert out["dialsup"][i] == pytest.approx(exp), f"row {i}: expected {exp}"
