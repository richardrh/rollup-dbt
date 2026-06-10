from __future__ import annotations

from dataclasses import dataclass

import polars as pl
import pandera.polars as pa

from rollup.columns import Col


@dataclass(frozen=True)
class MetricSpec:
    name: str
    loss_column: str


METRIC_LONG_SCHEMA = pa.DataFrameSchema(
    {
        Col.vendor: pa.Column(pl.String, nullable=True),
        Col.base_model: pa.Column(pl.String, nullable=True),
        Col.analysis_id: pa.Column(pl.String, nullable=True),
        Col.modelled_lob: pa.Column(pl.String, nullable=True),
        Col.modelled_peril: pa.Column(pl.String, nullable=True),
        Col.rollup_lob: pa.Column(pl.String, nullable=True),
        Col.rollup_peril: pa.Column(pl.String, nullable=True),
        Col.region_peril_id: pa.Column(pl.Int64, nullable=True),
        Col.class_: pa.Column(pl.String, nullable=True),
        Col.office: pa.Column(pl.String, nullable=True),
        Col.currency: pa.Column(pl.String, nullable=True),
        Col.target_currency: pa.Column(pl.String, nullable=True),
        Col.year_id: pa.Column(pl.Int64, nullable=True),
        Col.event_id: pa.Column(pl.Int64, nullable=True),
        Col.forecast_date: pa.Column(pl.String, nullable=True),
        Col.is_dialsup: pa.Column(pl.Int64, nullable=True),
        Col.metric: pa.Column(pl.String, nullable=True),
        Col.loss: pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)


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
