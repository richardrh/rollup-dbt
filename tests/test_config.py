from __future__ import annotations

from pathlib import Path

import pytest

from rollup.config import RollupConfig, read_config


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_read_config_without_path_reads_config_toml_from_cwd(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / "config.toml").write_text(
        (REPO_ROOT / "config.toml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    config = read_config()

    assert config.outputs.write_duckdb is True
    assert config.outputs.duckdb_file == "rollup.duckdb"
    assert config.outputs.minimum_event_loss_threshold == 1000.0
    assert config.logging.format == "jsonl"


def test_rollup_config_defaults_enable_duckdb_export() -> None:
    config = RollupConfig()

    assert config.outputs.write_duckdb is True


def test_read_config_rejects_unknown_section(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
        [unknown]
        value = "ignored"
        """,
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unknown config sections") as exc_info:
        read_config(config_path)

    assert "unknown" in str(exc_info.value)


def test_read_config_rejects_unknown_key(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
        [outputs]
        unexpected_key = "ignored"
        """,
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unknown config keys") as exc_info:
        read_config(config_path)

    assert "unexpected_key" in str(exc_info.value)


def test_read_config_rejects_unrelated_unknown_keys(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
        top_level_unknown = "ignored"

        [outputs]
        write_duckdb = false
        unrelated_unknown = "ignored"
        """,
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unknown config sections"):
        read_config(config_path)


def test_read_config_defaults_to_jsonl_when_config_file_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)

    config = read_config()

    assert config.logging.format == "jsonl"


@pytest.mark.parametrize(
    "body,match",
    [
        ('[analysis]\nreturn_periods = ["30"]\n', "return_periods"),
        ("[vendor_years]\nVerisk = 10000\nrisklink = 100000\n", "unknown config keys"),
        ("[vendor_years]\nverisk = 10000\n", "exactly verisk and risklink"),
        ('[vendor_years]\nverisk = "10000"\nrisklink = 100000\n', "must be an integer"),
        ('[blending]\nuplift_factor_min = "0.1"\n', "uplift_factor_min"),
        (
            '[outputs]\nminimum_event_loss_threshold = "1000"\n',
            "minimum_event_loss_threshold",
        ),
        ('[logging]\nformat = "json"\n', "logging format"),
        ('[logging]\nformat = "JSONL"\n', "logging format"),
        (
            '[blending]\ntarget_points = [{ ep_type = "aal", return_period = 0 }]\n',
            "ep_type",
        ),
        (
            '[blending]\ntarget_points = [{ ep_type = "AAL", return_period = 100 }]\n',
            "AAL",
        ),
    ],
)
def test_read_config_rejects_noncanonical_runtime_types(
    tmp_path: Path, body: str, match: str
) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(body, encoding="utf-8")

    with pytest.raises(ValueError, match=match):
        read_config(config_path)
