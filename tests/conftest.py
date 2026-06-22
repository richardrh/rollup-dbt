"""Pytest bootstrap.

Gate integration tests behind `--run-integration`. Tests marked
`@pytest.mark.integration` are SKIPPED by default. Pass the flag (or
    `-m integration`) to opt in. Integration tests typically require
external resources (Docker for SQL Server, network, etc.) and are
slower / heavier than the default unit suite.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import tomli_w

from rollup.config import RollupConfig, load_config


def pytest_addoption(parser):
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run integration tests (require Docker / external services)",
    )
    parser.addoption(
        "--run-fuzz",
        action="store_true",
        default=False,
        help="run property-based / fuzz tests (uses Hypothesis)",
    )


def pytest_collection_modifyitems(config, items):
    run_integration = config.getoption("--run-integration")
    run_fuzz = config.getoption("--run-fuzz")
    m_option = config.getoption("-m") or ""

    if not run_integration and "integration" not in m_option:
        skip_integration = pytest.mark.skip(reason="needs --run-integration (or -m integration)")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)

    if not run_fuzz and "fuzz" not in m_option:
        skip_fuzz = pytest.mark.skip(reason="needs --run-fuzz (or -m fuzz)")
        for item in items:
            if "fuzz" in item.keywords:
                item.add_marker(skip_fuzz)


BASE_CONFIG: dict[str, Any] = {
    "fx": {"target_currency": "GBP"},
    "outputs": {
        "write_stage_outputs": True,
        "write_duckdb": False,
        "duckdb_file": "rollup.duckdb",
        "stage_output_dir": "stages",
        "staging_dir": "staging",
        "intermediate_dir": "intermediate",
        "marts_dir": "marts",
        "analysis_dir": "analysis",
        "combined_file": "mts_tbl_ylt_combined_all_factors.parquet",
        "wide_file": "mts_tbl_ylt_combined_all_factors_wide.parquet",
        "dialsup_file": "mts_tbl_ylt_dialsup.parquet",
        "ep_report_file": "ep_report.csv",
        "fanout_prefixes": {"verisk": "HiscoAIR", "risklink": "HiscoRMS"},
    },
    "analysis": {"return_periods": [30, 200, 1000]},
    "vendor_years": {"verisk": 10000, "risklink": 100000},
    "blending": {
        "uplift_factor_min": 0.1,
        "uplift_factor_max": 10.0,
        "target_points": [
            {"ep_type": "AAL", "return_period": 0},
            {"ep_type": "OEP", "return_period": 200},
            {"ep_type": "OEP", "return_period": 1000},
        ],
        "subregion_selection": {"216": "216b"},
    },
}


def make_test_config_toml(**overrides: Any) -> str:
    """Return a complete TOML config string with optional overrides."""
    config = _deep_update(BASE_CONFIG, overrides)
    return tomli_w.dumps(config)


def _deep_update(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and key in result and isinstance(result[key], dict):
            result[key] = _deep_update(result[key], value)
        else:
            result[key] = value
    return result


@pytest.fixture
def rollup_config(tmp_path: Path) -> RollupConfig:
    """Provide a complete RollupConfig loaded from a temporary TOML file."""
    config_path = tmp_path / "rollup.toml"
    config_path.write_text(make_test_config_toml(), encoding="utf-8")
    return load_config(config_path)
