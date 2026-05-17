from __future__ import annotations

from pathlib import Path

import yaml

from rollup.pipeline2_schema import DEFAULT_SCHEMA_PATHS

REPO_ROOT = Path(__file__).resolve().parents[2]
LEGACY_CODE_SCHEMA_FILE = REPO_ROOT / "polars" / "rollup" / "pipeline2_schema.yaml"


def _load_raw_schema() -> dict:
    datasets = {}
    descriptions = []
    version = None
    for schema_file in DEFAULT_SCHEMA_PATHS:
        raw = yaml.safe_load(schema_file.read_text(encoding="utf-8"))
        if version is None:
            version = raw["version"]
        assert raw["version"] == version
        descriptions.append(raw["description"])
        datasets.update(raw["datasets"])
    return {"version": version, "description": " ".join(descriptions), "datasets": datasets}


def test_pipeline2_yaml_schema_lives_alongside_data_inputs_not_code() -> None:
    expected_schema_files = {
        REPO_ROOT / "data" / "seeds" / "schema.yaml",
        REPO_ROOT / "data" / "ylt" / "schema.yaml",
        REPO_ROOT / "data" / "ep_summaries" / "schema.yaml",
        REPO_ROOT / "data" / "output" / "schema.yaml",
    }

    assert set(DEFAULT_SCHEMA_PATHS) == expected_schema_files
    assert all(schema_file.exists() for schema_file in DEFAULT_SCHEMA_PATHS)
    assert all(schema_file.is_relative_to(REPO_ROOT / "data") for schema_file in DEFAULT_SCHEMA_PATHS)
    assert not LEGACY_CODE_SCHEMA_FILE.exists()


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
