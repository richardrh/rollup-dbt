from __future__ import annotations

from dataclasses import dataclass

from rollup.columns import Col


@dataclass(frozen=True)
class MetricSpec:
    name: str
    loss_column: str


def metric_specs(target_currency: str) -> tuple[MetricSpec, ...]:
    tag = _target_currency_tag(target_currency)
    return (
        MetricSpec("loss_original_ylt", Col.loss),
        MetricSpec("loss_blended", "blended_loss"),
        MetricSpec(f"loss_blended_fx_{tag}", "fx_loss"),
        MetricSpec(f"loss_blended_fx_{tag}_forecast", "forecast_loss"),
        MetricSpec(f"loss_blended_fx_{tag}_forecast_euws_override", "euws_loss"),
    )


def forecast_metric(target_currency: str) -> str:
    return f"loss_blended_fx_{_target_currency_tag(target_currency)}_forecast"


def final_main_metric(target_currency: str) -> str:
    return f"{forecast_metric(target_currency)}_euws_override"


def _target_currency_tag(target_currency: str) -> str:
    return str(target_currency).upper().lower()
