"""Canonical dbt-layer import checks."""

from __future__ import annotations

import ast
from pathlib import Path

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


REPO_ROOT = Path(__file__).resolve().parents[2]
LEGACY_REDIRECT_MODULES = (
    "rollup.stages.staging",
    "rollup.stages.factors",
    "rollup.stages.report",
    "rollup.stages.ep",
    "rollup.metrics.main_chain",
    "rollup.metrics.dialsup",
    "rollup.fanout",
    "rollup.variants",
)


def test_canonical_dbt_layer_packages_reexport_model_implementations() -> None:
    assert staging_normalize_verisk_ylt is ylt_normalize_verisk_ylt
    assert staging_ep_curve_from_ylt is ep_ep_curve_from_ylt
    assert intermediate_attach_currency is factors_attach_currency
    assert intermediate_add_main_metrics is metrics_add_main_metrics
    assert intermediate_add_dialsup is metrics_add_dialsup
    assert marts_fanout_hisco is hisco_fanout_hisco
    assert marts_variant_spec is variants_variant_spec
    assert reports_build_report is summary_build_report


def _iter_imported_modules(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.append(node.module)
    return modules


def test_production_and_tests_do_not_import_legacy_redirect_modules() -> None:
    checked_roots = (REPO_ROOT / "polars" / "rollup", REPO_ROOT / "polars" / "tests")

    offenders: list[str] = []
    for root in checked_roots:
        for path in root.rglob("*.py"):
            imported_modules = _iter_imported_modules(path)
            for module in imported_modules:
                for legacy_module in LEGACY_REDIRECT_MODULES:
                    if module == legacy_module or module.startswith(f"{legacy_module}."):
                        offenders.append(f"{path.relative_to(REPO_ROOT)} imports {module}")

    assert offenders == []
