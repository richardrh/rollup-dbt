from __future__ import annotations

from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_FILES = (
    REPO_ROOT / "data" / "seeds" / "schema.yaml",
    REPO_ROOT / "data" / "ylt" / "schema.yaml",
    REPO_ROOT / "data" / "ep_summaries" / "schema.yaml",
    REPO_ROOT / "data" / "output" / "schema.yaml",
)


def _load_raw_schema() -> dict:
    datasets = {}
    for schema_file in SCHEMA_FILES:
        raw = yaml.safe_load(schema_file.read_text(encoding="utf-8"))
        datasets.update(raw["datasets"])
    return {"datasets": datasets}


def test_pipeline2_schema_manifests_are_colocated_with_data_areas() -> None:
    assert not (REPO_ROOT / "data" / "pipeline2" / "schema.yaml").exists()
    assert all(schema_file.is_file() for schema_file in SCHEMA_FILES)


def test_pipeline2_yaml_schema_declares_required_sources() -> None:
    datasets = _load_raw_schema()["datasets"]

    expected = {
        "lobs",
        "perils",
        "analyses",
        "selected_analyses",
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


def test_seed_schema_paths_match_operator_layout() -> None:
    datasets = _load_raw_schema()["datasets"]

    assert datasets["lobs"]["path"] == "data/seeds/business/lobs.csv"
    assert datasets["perils"]["path"] == "data/seeds/business/perils.csv"
    assert datasets["analyses"]["path"] == "data/seeds/business/analyses.csv"
    assert datasets["selected_analyses"]["path"] == "data/seeds/business/selected_analyses.csv"
    assert datasets["blending_weights"]["path"] == "data/seeds/vor/blending_weights.csv"
    assert datasets["forecast_factors"]["path"] == "data/seeds/vor/forecast_factors.csv"
    assert datasets["fx_rates"]["path"] == "data/seeds/vor/fx_rates.csv"
    assert datasets["euws_rate_factors"]["path"] == "data/seeds/vor/euws_rate_factors.csv"


def test_ylt_ep_and_output_schema_paths_match_operator_layout() -> None:
    datasets = _load_raw_schema()["datasets"]

    assert datasets["raw_verisk_ylt"]["glob"] == "data/ylt/verisk/*.parquet"
    assert datasets["raw_risklink_ylt"]["glob"] == "data/ylt/risklink/*.parquet"
    assert datasets["canonical_ep_summary"]["glob"] == "data/ep_summaries/**/*.long.csv"
    assert datasets["mart_loss_summary"]["path"] == "data/output/pipeline2_loss_summary.parquet"


def test_selected_analyses_is_first_class() -> None:
    datasets = _load_raw_schema()["datasets"]

    assert datasets["selected_analyses"]["status"] == "first_class"
    assert datasets["selected_analyses"]["required"] is True


def test_selected_analyses_seed_template_exists_with_canonical_header() -> None:
    selected_analyses = REPO_ROOT / "data" / "seeds" / "business" / "selected_analyses.csv"

    assert selected_analyses.read_text(encoding="utf-8") == "vendor,analysis_id\n"


def test_pipeline2_yaml_columns_are_explicit_and_described() -> None:
    datasets = _load_raw_schema()["datasets"]

    for name, spec in datasets.items():
        assert spec["description"], name
        assert spec["format"], name
        assert spec["columns"], name
        for column in spec["columns"]:
            assert {"name", "dtype", "required", "description"} <= set(column), (name, column)
