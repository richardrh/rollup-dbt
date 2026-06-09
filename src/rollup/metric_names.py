from __future__ import annotations


LOSS_ORIGINAL_YLT = "loss_original_ylt"
LOSS_BLENDED = "loss_blended"


def normalize_target_currency(target_currency: str) -> str:
    return str(target_currency).upper()


def target_currency_tag(target_currency: str) -> str:
    return normalize_target_currency(target_currency).lower()


def loss_blended_fx_metric(target_currency: str) -> str:
    return f"loss_blended_fx_{target_currency_tag(target_currency)}"


def loss_blended_fx_forecast_metric(target_currency: str) -> str:
    return f"{loss_blended_fx_metric(target_currency)}_forecast"


def loss_blended_fx_forecast_euws_override_metric(target_currency: str) -> str:
    return f"{loss_blended_fx_forecast_metric(target_currency)}_euws_override"


def loss_dialsup_fx_forecast_metric(target_currency: str) -> str:
    return f"loss_dialsup_fx_{target_currency_tag(target_currency)}_forecast"
