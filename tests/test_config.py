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
    assert config.logging.format == "text"


def test_rollup_config_defaults_enable_duckdb_export() -> None:
    config = RollupConfig()

    assert config.outputs.write_duckdb is True
