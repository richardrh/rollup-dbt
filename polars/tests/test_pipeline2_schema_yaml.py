from __future__ import annotations

from pathlib import Path

import yaml


SCHEMA_FILE = Path(__file__).resolve().parents[1] / "rollup" / "pipeline2_schema.yaml"


def _load_raw_schema() -> dict:
    return yaml.safe_load(SCHEMA_FILE.read_text(encoding="utf-8"))


def test_pipeline2_yaml_schema_declares_required_sources() -> None:
    datasets = _load_raw_schema()["datasets"]

    expected = {
        "lobs",
        "perils",
        "analyses",
        "selected_analyses",
        "valid_analyses",
        "blending_weights",
        "forecast_factors",
        "fx_rates",
        "euws_rate_factors",
        "raw_verisk_ylt",
        "raw_risklink_ylt",
        "canonical_ep_summary",
        "stg_normalized_ylt",
        "int_selected_losses",
        "mart_loss_summary",
    }

    assert expected <= set(datasets)


def test_selected_analyses_is_first_class_and_valid_analyses_is_legacy_fallback() -> None:
    datasets = _load_raw_schema()["datasets"]

    assert datasets["selected_analyses"]["status"] == "first_class"
    assert datasets["selected_analyses"]["required"] is True
    assert datasets["valid_analyses"]["status"] == "legacy_fallback"
    assert datasets["valid_analyses"]["required"] is False


def test_pipeline2_yaml_columns_are_explicit_and_described() -> None:
    datasets = _load_raw_schema()["datasets"]

    for name, spec in datasets.items():
        assert spec["description"], name
        assert spec["format"], name
        assert spec["columns"], name
        for column in spec["columns"]:
            assert {"name", "dtype", "required", "description"} <= set(column), (name, column)
