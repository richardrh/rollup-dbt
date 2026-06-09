from __future__ import annotations

import polars as pl
import pytest

from rollup.columns import Col
from rollup.intermediate.apply_fx import apply_fx
from rollup.intermediate.build_metric_long import build_metric_long
from rollup.metric_names import (
    LOSS_BLENDED,
    LOSS_ORIGINAL_YLT,
    loss_blended_fx_forecast_euws_override_metric,
    loss_blended_fx_forecast_metric,
    loss_blended_fx_metric,
)


def test_apply_fx_uses_configured_target_currency_rates() -> None:
    frame = blended_frame(
        [
            {Col.currency: "GBP", "blended_loss": 100.0},
            {Col.currency: "EUR", "blended_loss": 100.0},
        ]
    )
    rates = pl.DataFrame(
        {
            "currency_code": ["GBP", "GBP", "EUR", "EUR"],
            Col.target_currency: ["GBP", "USD", "GBP", "USD"],
            "rate": [1.0, 1.25, 0.88, 1.1],
        }
    )

    result = apply_fx(frame.lazy(), rates, "GBP").collect().sort(Col.currency)

    assert result.select(Col.currency, Col.fx_rate, "gbp_loss", Col.target_currency).rows() == [
        ("EUR", 0.88, 88.0, "GBP"),
        ("GBP", 1.0, 100.0, "GBP"),
    ]


def test_apply_fx_uses_identity_for_missing_same_currency_rate() -> None:
    frame = blended_frame([{Col.currency: "GBP", "blended_loss": 50.0}])
    rates = pl.DataFrame(
        {
            "currency_code": ["EUR"],
            Col.target_currency: ["GBP"],
            "rate": [0.88],
        }
    )

    result = apply_fx(frame.lazy(), rates, "GBP").collect()

    assert result.select(Col.fx_rate, "gbp_loss", Col.target_currency).rows() == [(1.0, 50.0, "GBP")]


def test_apply_fx_raises_clear_error_for_missing_non_target_rate() -> None:
    frame = blended_frame([{Col.currency: "EUR", "blended_loss": 100.0}])
    rates = pl.DataFrame(
        {
            "currency_code": ["GBP"],
            Col.target_currency: ["GBP"],
            "rate": [1.0],
        }
    )

    with pytest.raises(ValueError, match="missing FX rates for currencies EUR targeting GBP"):
        apply_fx(frame.lazy(), rates, "GBP")


def test_build_metric_long_uses_default_gbp_lineage_metric_names() -> None:
    adjusted = blended_frame([{Col.currency: "GBP", "blended_loss": 10.0}]).with_columns(
        pl.lit(1.0).alias(Col.fx_rate),
        pl.lit("GBP").alias(Col.target_currency),
        pl.lit(10.0).alias("gbp_loss"),
        pl.lit("2026-01-01").alias(Col.forecast_date),
        pl.lit(1.0).alias(Col.forecast_factor),
        pl.lit(10.0).alias("forecast_loss"),
        pl.lit(1.0).alias(Col.euws_factor),
        pl.lit(10.0).alias("euws_loss"),
    )

    metrics = build_metric_long(adjusted.lazy()).collect().select(Col.metric).to_series().to_list()

    assert metrics == [
        LOSS_ORIGINAL_YLT,
        LOSS_BLENDED,
        loss_blended_fx_metric("GBP"),
        loss_blended_fx_forecast_metric("GBP"),
        loss_blended_fx_forecast_euws_override_metric("GBP"),
    ]


def blended_frame(overrides: list[dict[str, object]]) -> pl.DataFrame:
    base = {
        Col.vendor: "verisk",
        Col.analysis_id: "EQ",
        Col.modelled_lob: "Fine Art",
        Col.modelled_peril: "EQ",
        Col.model_code: 7,
        Col.year_id: 1,
        Col.event_id: 1,
        Col.loss: 10.0,
        Col.rollup_lob: "Fine Art",
        Col.rollup_peril: "Earthquake",
        Col.region_peril_id: 205,
        Col.class_: "ART",
        Col.office: "London",
        Col.currency: "GBP",
        Col.selection_priority: 1,
        Col.is_dialsup: 1,
        "blended_loss": 10.0,
    }
    return pl.DataFrame([{**base, **override} for override in overrides])
