from __future__ import annotations

from pathlib import Path

import pytest

from rollup.config import RollupConfig, load_config


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_load_config_without_path_reads_config_toml_from_cwd(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / "config.toml").write_text(
        (REPO_ROOT / "config.toml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    config = load_config()

    assert config.outputs.write_duckdb is True
    assert config.outputs.duckdb_file == "rollup.duckdb"
    assert config.outputs.minimum_event_loss_threshold == 1000.0
    assert config.outputs.fanout_prefixes == {
        "verisk": "HiscoAIR",
        "risklink": "HiscoRMS",
    }
    assert config.logging.format == "jsonl"


def test_rollup_config_defaults_enable_duckdb_export() -> None:
    config = RollupConfig()

    assert config.outputs.write_duckdb is True


def test_input_config_defaults_resolve_under_data_root() -> None:
    config = RollupConfig()
    data_root = Path("/tmp/example-data")

    assert config.inputs.verisk_events_path(data_root) == (
        data_root / "seeds" / "validation" / "verisk_events.parquet"
    )
    assert config.inputs.risklink_events_path(data_root) == (
        data_root / "seeds" / "validation" / "risklink_flood22_model_events.parquet"
    )


def test_load_config_parses_relative_input_paths_under_data_root(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
        [inputs]
        verisk_events_file = "custom/verisk.parquet"
        risklink_events_file = "custom/risklink.parquet"
        ignored = "value"
        """,
        encoding="utf-8",
    )

    config = load_config(config_path)
    data_root = tmp_path / "data"

    assert config.inputs.verisk_events_file == "custom/verisk.parquet"
    assert config.inputs.risklink_events_file == "custom/risklink.parquet"
    assert config.inputs.verisk_events_path(data_root) == (
        data_root / "custom" / "verisk.parquet"
    )
    assert config.inputs.risklink_events_path(data_root) == (
        data_root / "custom" / "risklink.parquet"
    )


def test_input_config_absolute_paths_are_preserved(tmp_path: Path) -> None:
    verisk_path = tmp_path / "verisk.parquet"
    risklink_path = tmp_path / "risklink.parquet"
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        f"""
        [inputs]
        verisk_events_file = "{verisk_path}"
        risklink_events_file = "{risklink_path}"
        """,
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.inputs.verisk_events_path(tmp_path / "data") == verisk_path
    assert config.inputs.risklink_events_path(tmp_path / "data") == risklink_path


def test_load_config_defaults_to_jsonl_when_config_file_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)

    config = load_config()

    assert config.logging.format == "jsonl"
