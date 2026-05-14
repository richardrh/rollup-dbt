"""Unit tests for the MAIN metric chain transformation."""

from __future__ import annotations

import polars as pl
import pytest

from rollup.chain import CHAIN_BASE, col_after, forecast_factor_col, main_loss_col
from rollup.intermediate.metrics import add_main_metrics
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import MetricCol as M
from rollup.schemas.columns import NormalizedYltCol as Y


def test_add_main_metrics_applies_chain_for_each_forecast_tag():
    tags = ["202601", "202607"]
    ylt = pl.DataFrame({
        Y.LOSS: [100.0],
        AF.UPLIFT_FACTOR: [0.80],
        AF.UPLIFT_FACTOR_CAPPED: [0.75],
        AF.RATE_TO_GBP: [1.50],
        forecast_factor_col("202601"): [1.10],
        forecast_factor_col("202607"): [1.20],
        AF.EUWS_FACTOR: [0.50],
    }).lazy()

    out = add_main_metrics(ylt, tags).collect()

    assert out[M.LOSS_UPLIFTED][0] == pytest.approx(80.0)
    assert out[M.LOSS_UPLIFTED_CAPPED][0] == pytest.approx(75.0)
    assert out[CHAIN_BASE][0] == pytest.approx(50.0)

    assert out[col_after("forecast", "202601")][0] == pytest.approx(55.0)
    assert out[col_after("euws", "202601")][0] == pytest.approx(27.5)
    assert out[main_loss_col("202601")][0] == pytest.approx(27.5)

    assert out[col_after("forecast", "202607")][0] == pytest.approx(60.0)
    assert out[col_after("euws", "202607")][0] == pytest.approx(30.0)
    assert out[main_loss_col("202607")][0] == pytest.approx(30.0)


def test_add_main_metrics_uses_capped_uplift_for_local_currency_base():
    tag = "202601"
    ylt = pl.DataFrame({
        Y.LOSS: [100.0],
        AF.UPLIFT_FACTOR: [9.0],
        AF.UPLIFT_FACTOR_CAPPED: [2.0],
        AF.RATE_TO_GBP: [4.0],
        forecast_factor_col(tag): [1.0],
        AF.EUWS_FACTOR: [1.0],
    }).lazy()

    out = add_main_metrics(ylt, [tag]).collect()

    assert out[M.LOSS_UPLIFTED][0] == pytest.approx(900.0)
    assert out[M.LOSS_UPLIFTED_CAPPED][0] == pytest.approx(200.0)
    assert out[CHAIN_BASE][0] == pytest.approx(50.0)
    assert out[main_loss_col(tag)][0] == pytest.approx(50.0)


def test_add_main_metrics_keeps_forecast_tags_independent():
    tags = ["202601", "202607"]
    ylt = pl.DataFrame({
        Y.LOSS: [10.0],
        AF.UPLIFT_FACTOR: [1.0],
        AF.UPLIFT_FACTOR_CAPPED: [1.0],
        AF.RATE_TO_GBP: [1.0],
        forecast_factor_col("202601"): [2.0],
        forecast_factor_col("202607"): [3.0],
        AF.EUWS_FACTOR: [5.0],
    }).lazy()

    out = add_main_metrics(ylt, tags).collect()

    assert out[main_loss_col("202601")][0] == pytest.approx(10.0 * 2.0 * 5.0)
    assert out[main_loss_col("202607")][0] == pytest.approx(10.0 * 3.0 * 5.0)
