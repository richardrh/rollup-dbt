"""Compatibility checks for pre-dbt-layer import paths."""

from __future__ import annotations

from rollup.fanout import fanout_hisco as legacy_fanout_hisco
from rollup.intermediate.factors import attach_currency as layer_attach_currency
from rollup.intermediate.metrics import add_dialsup as layer_add_dialsup
from rollup.intermediate.metrics import add_main_metrics as layer_add_main_metrics
from rollup.marts import VariantSpec as layer_variant_spec
from rollup.marts import fanout_hisco as layer_fanout_hisco
from rollup.metrics.dialsup import add_dialsup as legacy_add_dialsup
from rollup.metrics.main_chain import add_main_metrics as legacy_add_main_metrics
from rollup.reports import build_report as layer_build_report
from rollup.stages.factors import attach_currency as legacy_attach_currency
from rollup.stages.report import build_report as legacy_build_report
from rollup.stages.staging import normalize_verisk_ylt as legacy_normalize_verisk_ylt
from rollup.staging import normalize_verisk_ylt as layer_normalize_verisk_ylt
from rollup.variants import VariantSpec as legacy_variant_spec


def test_legacy_import_paths_reexport_layer_implementations() -> None:
    assert legacy_normalize_verisk_ylt is layer_normalize_verisk_ylt
    assert legacy_attach_currency is layer_attach_currency
    assert legacy_add_main_metrics is layer_add_main_metrics
    assert legacy_add_dialsup is layer_add_dialsup
    assert legacy_fanout_hisco is layer_fanout_hisco
    assert legacy_variant_spec is layer_variant_spec
    assert legacy_build_report is layer_build_report
