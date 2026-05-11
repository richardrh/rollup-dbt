"""Unit tests for the DIALSUP metric transformation."""

from __future__ import annotations

import polars as pl
import pytest

from rollup.metrics.dialsup import add_dialsup
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import NormalizedYltCol as Y


def test_add_dialsup_single_column_named_dialsup():
    """add_dialsup adds exactly one column called 'dialsup', not per-tag."""
    ylt = pl.DataFrame({
        Y.LOSS:         [100.0],
        AF.RATE_TO_GBP: [1.25],
    }).lazy()

    out = add_dialsup(ylt).collect()

    assert "dialsup" in out.columns
    assert not any(col.startswith("dialsup_") for col in out.columns)


def test_add_dialsup_equals_loss_div_fx():
    """dialsup == loss / rate_to_gbp exactly — currency conversion only."""
    ylt = pl.DataFrame({
        Y.LOSS:         [1000.0, 500.0, 250.0],
        AF.RATE_TO_GBP: [1.25,   0.88,  1.0],
    }).lazy()

    out = add_dialsup(ylt).collect()

    assert out["dialsup"][0] == pytest.approx(1000.0 / 1.25)
    assert out["dialsup"][1] == pytest.approx(500.0  / 0.88)
    assert out["dialsup"][2] == pytest.approx(250.0  / 1.0)


def test_dialsup_equals_loss_div_fx_for_each_row():
    """Synthetic AllFactors frame: every row's dialsup equals loss / rate_to_gbp."""
    ylt = pl.DataFrame({
        Y.LOSS:         [800.0, 200.0],
        AF.RATE_TO_GBP: [0.80,  1.00],
    }).lazy()

    out = add_dialsup(ylt).collect()
    expected = [800.0 / 0.80, 200.0 / 1.00]

    for i, exp in enumerate(expected):
        assert out["dialsup"][i] == pytest.approx(exp), f"row {i}: expected {exp}"
