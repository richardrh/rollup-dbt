from __future__ import annotations

from pathlib import Path

import yaml
from validnator.pipeline_config import PipelineConfig


VALIDNATOR_CONFIG_DIR = Path(__file__).resolve().parents[1] / "validation" / "validnator"


def test_validnator_pipeline_configs_exist() -> None:
    expected = {
        "blending_factors.yml",
        "ep_summary_long.yml",
        "euws_rate_factors.yml",
        "forecast_factors.yml",
        "fx_rates.yml",
        "lobs.yml",
        "perils.yml",
        "risklink_ylt_csv.yml",
        "verisk_ylt_csv.yml",
    }

    assert {path.name for path in VALIDNATOR_CONFIG_DIR.glob("*.yml")} == expected


def test_validnator_pipeline_configs_parse_with_pyyaml_and_validnator() -> None:
    for config_path in sorted(VALIDNATOR_CONFIG_DIR.glob("*.yml")):
        raw_config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        assert raw_config["input"] == {"type": "csv", "mode": "raw_strings"}

        config = PipelineConfig.from_yaml(config_path)

        assert config.name
        assert config.input.input_type == "csv"
        assert config.schema_gate
