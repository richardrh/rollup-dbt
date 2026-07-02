from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_FILES = (
    REPO_ROOT / "data" / "seeds" / "schema.yaml",
    REPO_ROOT / "data" / "ylt" / "schema.yaml",
    REPO_ROOT / "data" / "ep_summaries" / "schema.yaml",
)
VALIDNATOR_FILES = (
    REPO_ROOT / "data" / "ylt" / "validnator-verisk.yml",
    REPO_ROOT / "data" / "ylt" / "validnator-risklink.yml",
    REPO_ROOT / "data" / "ep_summaries" / "validnator.yml",
    REPO_ROOT / "data" / "seeds" / "business" / "validnator-lobs.yml",
    REPO_ROOT / "data" / "seeds" / "business" / "validnator-perils.yml",
    REPO_ROOT / "data" / "seeds" / "vor" / "validnator-blending-factors.yml",
    REPO_ROOT / "data" / "seeds" / "vor" / "validnator-fx-rates.yml",
    REPO_ROOT / "data" / "seeds" / "vor" / "validnator-forecast-factors.yml",
    REPO_ROOT / "data" / "seeds" / "vor" / "validnator-euws-rate-factors.yml",
    REPO_ROOT / "data" / "seeds" / "adjustments" / "validnator.yml",
    REPO_ROOT / "data" / "seeds" / "validation" / "validnator-verisk-events.yml",
    REPO_ROOT / "data" / "seeds" / "validation" / "validnator-risklink-flood-events.yml",
)


def test_schema_yaml_files_are_removed() -> None:
    assert all(not schema_file.exists() for schema_file in SCHEMA_FILES)


def test_validnator_contracts_are_present() -> None:
    assert all(path.is_file() for path in VALIDNATOR_FILES)


def test_risklink_flood_contract_requires_model_occurrence_year() -> None:
    text = (REPO_ROOT / "data" / "seeds" / "validation" / "validnator-risklink-flood-events.yml").read_text(
        encoding="utf-8"
    )
    assert "ModelOccurrenceYear: int64" in text
    assert "- ModelOccurrenceYear" in text
    assert "min: 0" in text
