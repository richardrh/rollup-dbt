from __future__ import annotations

from pathlib import Path

import pytest

from rollup.config import RollupConfig, load_config


def test_load_config_defaults_to_tracked_config_toml(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / "config.toml").write_text(
        """
[fx]
target_currency = "usd"

[outputs]
write_stage_outputs = false
""".strip(),
        encoding="utf-8",
    )
    (tmp_path / "rollup.local.toml").write_text(
        """
[fx]
target_currency = "eur"
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    config = load_config()

    assert config.fx.target_currency == "USD"
    assert config.outputs.write_stage_outputs is False


def test_load_config_still_accepts_explicit_local_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / "config.toml").write_text(
        """
[fx]
target_currency = "gbp"
""".strip(),
        encoding="utf-8",
    )
    local_config = tmp_path / "rollup.local.toml"
    local_config.write_text(
        """
[fx]
target_currency = "eur"
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    config = load_config(local_config)

    assert config.fx.target_currency == "EUR"


def test_load_config_uses_dataclass_defaults_when_config_toml_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)

    assert load_config() == RollupConfig()


def test_output_config_defaults_minimum_event_loss_threshold_to_1000() -> None:
    assert RollupConfig().outputs.minimum_event_loss_threshold == 1000.0


def test_load_config_casts_minimum_event_loss_threshold_override(tmp_path: Path) -> None:
    config_path = tmp_path / "rollup.toml"
    config_path.write_text(
        """
[outputs]
minimum_event_loss_threshold = 250
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.outputs.minimum_event_loss_threshold == 250.0
    assert isinstance(config.outputs.minimum_event_loss_threshold, float)
