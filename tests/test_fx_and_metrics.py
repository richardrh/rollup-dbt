from __future__ import annotations

import polars as pl
import pytest

from rollup.columns import Col
from rollup.intermediate.apply_forecast import apply_forecast
from rollup.intermediate.apply_fx import apply_fx
from rollup.intermediate.build_dialsup import build_dialsup
from rollup.intermediate.build_metric_long import build_metric_long
from rollup.metric_names import (
    LOSS_BLENDED,
    LOSS_ORIGINAL_YLT,
    loss_blended_fx_forecast_euws_override_metric,
    loss_blended_fx_forecast_metric,
    loss_blended_fx_metric,
    loss_dialsup_fx_forecast_metric,
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

    assert result.select(Col.currency, Col.fx_rate, "fx_loss", Col.target_currency).rows() == [
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

    assert result.select(Col.fx_rate, "fx_loss", Col.target_currency).rows() == [(1.0, 50.0, "GBP")]


def test_apply_fx_requires_target_currency_for_non_empty_usd_rates() -> None:
    frame = blended_frame([{Col.currency: "EUR", "blended_loss": 100.0}])
    rates = pl.DataFrame(
        {
            "currency_code": ["EUR"],
            "rate": [1.1],
        }
    )

    with pytest.raises(ValueError, match="FX rates must include target_currency column"):
        apply_fx(frame.lazy(), rates, "USD")


def test_apply_fx_requires_target_currency_for_non_empty_gbp_rates() -> None:
    frame = blended_frame([{Col.currency: "EUR", "blended_loss": 100.0}])
    rates = pl.DataFrame(
        {
            "currency_code": ["EUR"],
            "rate": [0.88],
        }
    )

    with pytest.raises(ValueError, match="FX rates must include target_currency column"):
        apply_fx(frame.lazy(), rates, "GBP")


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


def test_apply_forecast_cross_joins_dates_and_defaults_missing_class_office_factor() -> None:
    frame = blended_frame(
        [
            {
                Col.class_: "FA",
                Col.office: "UK",
                Col.fx_rate: 1.0,
                Col.target_currency: "GBP",
                "fx_loss": 100.0,
            }
        ]
    )
    forecast_factors = pl.DataFrame(
        {
            Col.class_: ["PROP", "PROP", "PROP"],
            Col.office: ["US", "US", "US"],
            Col.forecast_date: ["2026-01-01", "2026-07-01", "2026-12-31"],
            "factor": [1.1, 1.2, 1.3],
        }
    )

    result = apply_forecast(frame.lazy(), forecast_factors).collect().sort(Col.forecast_date)

    assert result.select(Col.forecast_date, Col.forecast_factor, "forecast_loss").rows() == [
        ("2026-01-01", 1.0, 100.0),
        ("2026-07-01", 1.0, 100.0),
        ("2026-12-31", 1.0, 100.0),
    ]


def test_build_metric_long_uses_default_gbp_lineage_metric_names() -> None:
    adjusted = blended_frame([{Col.currency: "GBP", "blended_loss": 10.0}]).with_columns(
        pl.lit(1.0).alias(Col.fx_rate),
        pl.lit("GBP").alias(Col.target_currency),
        pl.lit(10.0).alias("fx_loss"),
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


def test_build_dialsup_uses_original_ylt_loss_fx_and_forecast() -> None:
    adjusted = blended_frame(
        [
            {
                Col.loss: 100.0,
                "blended_loss": 10.0,
                Col.fx_rate: 2.0,
                Col.target_currency: "GBP",
                "fx_loss": 20.0,
                Col.forecast_date: "2026-01-01",
                Col.forecast_factor: 3.0,
                "forecast_loss": 30.0,
                Col.euws_factor: 1.0,
                "euws_loss": 30.0,
                Col.is_dialsup: 1,
            }
        ]
    )

    result = build_dialsup(adjusted.lazy()).collect()

    assert result.select(Col.metric, Col.loss).rows() == [
        (loss_dialsup_fx_forecast_metric("GBP"), 600.0)
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
        Col.base_model: "verisk",
        Col.rnk: 1,
        Col.rp: 10000.0,
        Col.rp_bucket: 1000,
        Col.risklink_loss: 5.0,
        Col.verisk_loss: 10.0,
        Col.target_loss: 10.0,
        Col.base_model_loss: 10.0,
        Col.uplift_factor_on_base_model: 1.0,
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
