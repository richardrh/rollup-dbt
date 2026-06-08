from __future__ import annotations

from pathlib import Path

import polars as pl
import yaml
from validnator.config import ValidationConfig
from validnator.pipeline import Pipeline
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
        Path("data/seeds/validation/validnator-risklink-flood-events.yml"),
        Path("data/seeds/validation/validnator-verisk-events.yml"),
        Path("data/ylt/validnator-risklink.yml"),
        Path("data/ylt/validnator-verisk.yml"),
    }

    assert {path.relative_to(REPO_ROOT) for path in _validnator_configs()} == expected


def test_validnator_pipeline_configs_are_not_centralized() -> None:
    assert not list(CENTRAL_VALIDNATOR_DIR.glob("*.yml"))


def test_validnator_pipeline_configs_parse_with_pyyaml_and_validnator() -> None:
    for config_path in _validnator_configs():
        raw_config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        raw_input = raw_config.get("input")
        if raw_input is not None:
            assert raw_input["type"] == "csv"
            assert raw_input["mode"] in {"infer_types", "raw_strings"}

        config = PipelineConfig.from_yaml(config_path)

        assert config.name
        assert config.input.input_type == "csv"
        assert config.input.mode in {"infer_types", "raw_strings"}
        assert config.schema_gate


def test_parquet_validnator_configs_accept_current_catalogue_schemas(tmp_path: Path) -> None:
    cases = [
        (
            DATA_DIR / "seeds" / "validation" / "validnator-verisk-events.yml",
            DATA_DIR / "seeds" / "validation" / "verisk_events.parquet",
        ),
        (
            DATA_DIR / "seeds" / "validation" / "validnator-risklink-flood-events.yml",
            DATA_DIR / "seeds" / "validation" / "risklink_flood22_model_events.parquet",
        ),
    ]

    for config_path, parquet_path in cases:
        pipeline = Pipeline.from_config(
            ValidationConfig(
                input_file=parquet_path,
                output_dir=tmp_path / config_path.stem,
                pipeline_config_file=config_path,
            )
        )
        df = pl.read_parquet(parquet_path, n_rows=5)

        results = pipeline.run_with_df(df)

        assert results.schema_gate_errors is None
        assert results.errored.is_empty()


def test_readmes_do_not_contain_stale_parquet_validnator_claims() -> None:
    readmes = [
        DATA_DIR / "ylt" / "README.md",
        DATA_DIR / "seeds" / "README.md",
        DATA_DIR / "seeds" / "validation" / "README.md",
    ]
    stale_claims = [
        "validnator does not validate parquet",
        "does **not** validate parquet",
        "csv-equivalent checks",
    ]

    for readme in readmes:
        text = readme.read_text(encoding="utf-8").lower()
        for stale_claim in stale_claims:
            assert stale_claim not in text
