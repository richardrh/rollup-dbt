from __future__ import annotations

from pathlib import Path

import yaml
from validnator.pipeline_config import PipelineConfig


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
CENTRAL_VALIDNATOR_DIR = REPO_ROOT / "validation" / "validnator"


def _validnator_configs() -> list[Path]:
    return sorted(DATA_DIR.glob("**/validnator*.yml"))


def test_validnator_pipeline_configs_exist() -> None:
    expected = {
        Path("data/ep_summaries/validnator.yml"),
        Path("data/seeds/adjustments/validnator.yml"),
        Path("data/seeds/business/validnator-lobs.yml"),
        Path("data/seeds/business/validnator-perils.yml"),
        Path("data/seeds/vor/validnator-blending-factors.yml"),
        Path("data/seeds/vor/validnator-euws-rate-factors.yml"),
        Path("data/seeds/vor/validnator-forecast-factors.yml"),
        Path("data/seeds/vor/validnator-fx-rates.yml"),
        Path("data/ylt/validnator-risklink.yml"),
        Path("data/ylt/validnator-verisk.yml"),
    }

    assert {path.relative_to(REPO_ROOT) for path in _validnator_configs()} == expected


def test_validnator_pipeline_configs_are_not_centralized() -> None:
    assert not list(CENTRAL_VALIDNATOR_DIR.glob("*.yml"))


def test_validnator_pipeline_configs_parse_with_pyyaml_and_validnator() -> None:
    for config_path in _validnator_configs():
        raw_config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        assert raw_config["input"] == {"type": "csv", "mode": "raw_strings"}

        config = PipelineConfig.from_yaml(config_path)

        assert config.name
        assert config.input.input_type == "csv"
        assert config.schema_gate
