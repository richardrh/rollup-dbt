from __future__ import annotations

from pathlib import Path

import pytest

from rollup import cli
from rollup.api import RollupOutputPaths, RollupRunResult


def test_cli_run_uses_local_defaults(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    calls: dict[str, object] = {}

    def configure_console_logging(log_level: str, *, log_file: Path | None = None) -> None:
        calls["logging"] = (log_level, log_file)

    def run_rollup(
        data_root: Path,
        output_root: Path,
        *,
        config_path: Path | None,
        config: object | None,
        write_analysis: bool,
        log_file: Path,
    ) -> RollupRunResult:
        calls["run"] = {
            "data_root": data_root,
            "output_root": output_root,
            "config_path": config_path,
            "config": config,
            "write_analysis": write_analysis,
            "log_file": log_file,
        }
        return rollup_result(data_root, output_root, ep_report_path=output_root / "analysis" / "ep_report.csv")

    monkeypatch.setattr(cli, "configure_console_logging", configure_console_logging)
    monkeypatch.setattr(cli, "run_rollup", run_rollup)

    assert cli.main(["run"]) == 0

    assert calls["logging"] == ("INFO", Path("output") / "rollup.log")
    assert calls["run"] == {
        "data_root": Path("data"),
        "output_root": Path("output"),
        "config_path": None,
        "config": None,
        "write_analysis": True,
        "log_file": Path("output") / "rollup.log",
    }
    summary = capsys.readouterr().out
    assert "Rollup complete" in summary
    assert "combined mart: output/marts/combined.parquet" in summary
    assert "analysis report: output/analysis/ep_report.csv" in summary


def test_cli_run_applies_overrides_without_rewriting_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "rollup.toml"
    config_path.write_text(
        """
[outputs]
write_stage_outputs = true
combined_file = "custom-combined.parquet"
""".strip(),
        encoding="utf-8",
    )
    log_file = tmp_path / "logs" / "rollup.log"
    calls: dict[str, object] = {}

    def configure_console_logging(log_level: str, *, log_file: Path | None = None) -> None:
        calls["logging"] = (log_level, log_file)

    def run_rollup(
        data_root: Path,
        output_root: Path,
        *,
        config_path: Path | None,
        config: object | None,
        write_analysis: bool,
        log_file: Path,
    ) -> RollupRunResult:
        calls["run"] = {
            "data_root": data_root,
            "output_root": output_root,
            "config_path": config_path,
            "config": config,
            "write_analysis": write_analysis,
            "log_file": log_file,
        }
        return rollup_result(data_root, output_root, ep_report_path=None, stage_dir=None)

    monkeypatch.setattr(cli, "configure_console_logging", configure_console_logging)
    monkeypatch.setattr(cli, "run_rollup", run_rollup)

    data_root = tmp_path / "data"
    output_root = tmp_path / "output"

    assert (
        cli.main(
            [
                "run",
                "--data-root",
                str(data_root),
                "--output-root",
                str(output_root),
                "--config-path",
                str(config_path),
                "--no-analysis",
                "--no-stage-outputs",
                "--target-currency",
                "usd",
                "--log-level",
                "debug",
                "--log-file",
                str(log_file),
            ]
        )
        == 0
    )

    run_call = calls["run"]
    assert isinstance(run_call, dict)
    config = run_call["config"]
    assert calls["logging"] == ("DEBUG", log_file)
    assert run_call["data_root"] == data_root
    assert run_call["output_root"] == output_root
    assert run_call["config_path"] is None
    assert run_call["write_analysis"] is False
    assert run_call["log_file"] == log_file
    assert config is not None
    assert config.outputs.write_stage_outputs is False
    assert config.outputs.combined_file == "custom-combined.parquet"
    assert config.fx.target_currency == "USD"
    assert "write_stage_outputs = true" in config_path.read_text(encoding="utf-8")
    summary = capsys.readouterr().out
    assert "analysis report: (disabled)" in summary
    assert "stage outputs: (disabled)" in summary


def rollup_result(
    data_root: Path,
    output_root: Path,
    *,
    ep_report_path: Path | None,
    stage_dir: Path | None | object = ...,
) -> RollupRunResult:
    stage_path = output_root / "stages" if stage_dir is ... else stage_dir
    return RollupRunResult(
        data_root=data_root,
        output_root=output_root,
        outputs=RollupOutputPaths(
            mts_combined=output_root / "marts" / "combined.parquet",
            mts_wide=output_root / "marts" / "wide.parquet",
            mts_dialsup=output_root / "marts" / "dialsup.parquet",
            event_validation=output_root / "marts" / "event-validation.parquet",
            marts_dir=output_root / "marts",
            mart_files=(),
            stage_dir=stage_path,
        ),
        ep_report_path=ep_report_path,
    )
