"""Canonical dbt-layer import checks."""

from __future__ import annotations

from rollup.intermediate import add_dialsup as intermediate_add_dialsup
from rollup.intermediate import add_main_metrics as intermediate_add_main_metrics
from rollup.intermediate import attach_currency as intermediate_attach_currency
from rollup.intermediate.factors import attach_currency as factors_attach_currency
from rollup.intermediate.metrics import add_dialsup as metrics_add_dialsup
from rollup.intermediate.metrics import add_main_metrics as metrics_add_main_metrics
from rollup.marts import VariantSpec as marts_variant_spec
from rollup.marts import fanout_hisco as marts_fanout_hisco
from rollup.marts.hisco import fanout_hisco as hisco_fanout_hisco
from rollup.marts.variants import VariantSpec as variants_variant_spec
from rollup.reports import build_report as reports_build_report
from rollup.reports.summary import build_report as summary_build_report
from rollup.staging import ep_curve_from_ylt as staging_ep_curve_from_ylt
from rollup.staging import normalize_verisk_ylt as staging_normalize_verisk_ylt
from rollup.staging.ep import ep_curve_from_ylt as ep_ep_curve_from_ylt
from rollup.staging.ylt import normalize_verisk_ylt as ylt_normalize_verisk_ylt


def test_canonical_dbt_layer_packages_reexport_model_implementations() -> None:
    assert staging_normalize_verisk_ylt is ylt_normalize_verisk_ylt
    assert staging_ep_curve_from_ylt is ep_ep_curve_from_ylt
    assert intermediate_attach_currency is factors_attach_currency
    assert intermediate_add_main_metrics is metrics_add_main_metrics
    assert intermediate_add_dialsup is metrics_add_dialsup
    assert marts_fanout_hisco is hisco_fanout_hisco
    assert marts_variant_spec is variants_variant_spec
    assert reports_build_report is summary_build_report
