from __future__ import annotations

import json
import logging
from pathlib import Path

import polars as pl
import pytest

from rollup import cli
from rollup.api import (
    RollupOutputPaths,
    RollupRunResult,
)


def test_cli_run_uses_local_defaults(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    calls: dict[str, object] = {}

    def configure_console_logging(
        log_level: str, *, log_file: Path | None = None, log_format: str = "text"
    ) -> None:
        calls["logging"] = (log_level, log_file, log_format)

    def run_rollup(
        data_root: Path,
        output_root: Path,
        *,
        config_path: Path | None,
        config: object | None,
        write_analysis: bool,
        log_file: Path,
        log_format: str | None = None,
    ) -> RollupRunResult:
        calls["run"] = {
            "data_root": data_root,
            "output_root": output_root,
            "config_path": config_path,
            "config": config,
            "write_analysis": write_analysis,
            "log_file": log_file,
            "log_format": log_format,
        }
        return rollup_result(
            data_root,
            output_root,
            ep_report_path=output_root / "analysis" / "ep_report.csv",
        )

    monkeypatch.setattr(cli, "configure_console_logging", configure_console_logging)
    monkeypatch.setattr(cli, "run_rollup", run_rollup)

    assert cli.main(["run"]) == 0

    assert calls["logging"] == ("INFO", Path("output") / "rollup.log", "text")
    assert calls["run"] == {
        "data_root": Path("data"),
        "output_root": Path("output"),
        "config_path": None,
        "config": None,
        "write_analysis": True,
        "log_file": Path("output") / "rollup.log",
        "log_format": "text",
    }
    summary = capsys.readouterr().out
    assert "Rollup complete" in summary
    assert (
        f"combined mart: {(tmp_path / 'output' / 'marts' / 'combined.parquet')}"
        in summary
    )
    assert (
        f"analysis report: {(tmp_path / 'output' / 'analysis' / 'ep_report.csv')} (missing)"
        in summary
    )
    assert "WARNING: analysis report missing:" in summary


def test_cli_run_log_format_json_writes_json_lines(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    log_file = tmp_path / "logs" / "rollup.log"

    def run_rollup(
        data_root: Path,
        output_root: Path,
        *,
        config_path: Path | None,
        config: object | None,
        write_analysis: bool,
        log_file: Path,
        log_format: str | None = None,
    ) -> RollupRunResult:
        logging.getLogger("rollup.test").info("json log from cli")
        return rollup_result(data_root, output_root, ep_report_path=None)

    monkeypatch.setattr(cli, "run_rollup", run_rollup)

    assert cli.main(["run", "--log-file", str(log_file), "--log-format", "json"]) == 0

    line = log_file.read_text(encoding="utf-8").splitlines()[0]
    payload = json.loads(line)
    assert payload["level"] == "INFO"
    assert payload["logger"] == "rollup.test"
    assert payload["message"] == "json log from cli"
    assert "timestamp" in payload


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

    def configure_console_logging(
        log_level: str, *, log_file: Path | None = None, log_format: str = "text"
    ) -> None:
        calls["logging"] = (log_level, log_file, log_format)

    def run_rollup(
        data_root: Path,
        output_root: Path,
        *,
        config_path: Path | None,
        config: object | None,
        write_analysis: bool,
        log_file: Path,
        log_format: str | None = None,
    ) -> RollupRunResult:
        calls["run"] = {
            "data_root": data_root,
            "output_root": output_root,
            "config_path": config_path,
            "config": config,
            "write_analysis": write_analysis,
            "log_file": log_file,
            "log_format": log_format,
        }
        return rollup_result(
            data_root, output_root, ep_report_path=None, stage_dir=None
        )

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
    assert calls["logging"] == ("DEBUG", log_file, "text")
    assert run_call["data_root"] == data_root
    assert run_call["output_root"] == output_root
    assert run_call["config_path"] is None
    assert run_call["write_analysis"] is False
    assert run_call["log_file"] == log_file
    assert run_call["log_format"] == "text"
    assert config is not None
    assert config.outputs.write_stage_outputs is False
    assert config.outputs.combined_file == "custom-combined.parquet"
    assert config.fx.target_currency == "USD"
    assert "write_stage_outputs = true" in config_path.read_text(encoding="utf-8")
    summary = capsys.readouterr().out
    assert "analysis report: (disabled)" in summary
    assert "stage outputs: (disabled)" in summary


def test_cli_run_duckdb_flag_enables_default_duckdb_output(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: dict[str, object] = {}

    def configure_console_logging(
        log_level: str, *, log_file: Path | None = None, log_format: str = "text"
    ) -> None:
        calls["logging"] = (log_level, log_file, log_format)

    def run_rollup(
        data_root: Path,
        output_root: Path,
        *,
        config_path: Path | None,
        config: object | None,
        write_analysis: bool,
        log_file: Path,
        log_format: str | None = None,
    ) -> RollupRunResult:
        calls["run"] = {
            "data_root": data_root,
            "output_root": output_root,
            "config_path": config_path,
            "config": config,
            "write_analysis": write_analysis,
            "log_file": log_file,
            "log_format": log_format,
        }
        return rollup_result(
            data_root,
            output_root,
            ep_report_path=None,
            duckdb_file=output_root / "rollup.duckdb",
        )

    monkeypatch.setattr(cli, "configure_console_logging", configure_console_logging)
    monkeypatch.setattr(cli, "run_rollup", run_rollup)

    output_root = tmp_path / "output"

    assert cli.main(["run", "--output-root", str(output_root), "--duckdb"]) == 0

    run_call = calls["run"]
    assert isinstance(run_call, dict)
    config = run_call["config"]
    assert config is not None
    assert config.outputs.write_duckdb is True
    assert config.outputs.duckdb_file == "rollup.duckdb"
    assert run_call["config_path"] is None
    assert (
        f"duckdb: {output_root / 'rollup.duckdb'} (missing)" in capsys.readouterr().out
    )


def test_cli_run_duckdb_file_overrides_path_and_implies_enabled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: dict[str, object] = {}

    def configure_console_logging(
        log_level: str, *, log_file: Path | None = None, log_format: str = "text"
    ) -> None:
        calls["logging"] = (log_level, log_file, log_format)

    def run_rollup(
        data_root: Path,
        output_root: Path,
        *,
        config_path: Path | None,
        config: object | None,
        write_analysis: bool,
        log_file: Path,
        log_format: str | None = None,
    ) -> RollupRunResult:
        calls["run"] = {"config": config, "config_path": config_path}
        return rollup_result(
            data_root, output_root, ep_report_path=None, duckdb_file=duckdb_file
        )

    monkeypatch.setattr(cli, "configure_console_logging", configure_console_logging)
    monkeypatch.setattr(cli, "run_rollup", run_rollup)
    duckdb_file = tmp_path / "custom" / "my_rollup.duckdb"

    assert cli.main(["run", "--duckdb-file", str(duckdb_file)]) == 0

    run_call = calls["run"]
    assert isinstance(run_call, dict)
    config = run_call["config"]
    assert config is not None
    assert config.outputs.write_duckdb is True
    assert config.outputs.duckdb_file == str(duckdb_file)
    assert run_call["config_path"] is None
    assert f"duckdb: {duckdb_file} (missing)" in capsys.readouterr().out


def test_cli_run_error_propagates_without_validation_message(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def configure_console_logging(
        log_level: str, *, log_file: Path | None = None, log_format: str = "text"
    ) -> None:
        return None

    def run_rollup(
        data_root: Path,
        output_root: Path,
        *,
        config_path: Path | None,
        config: object | None,
        write_analysis: bool,
        log_file: Path,
        log_format: str | None = None,
    ) -> RollupRunResult:
        raise RuntimeError("unexpected load bug")

    monkeypatch.setattr(cli, "configure_console_logging", configure_console_logging)
    monkeypatch.setattr(cli, "run_rollup", run_rollup)

    with pytest.raises(RuntimeError, match="unexpected load bug"):
        cli.main(["run"])

    captured = capsys.readouterr()
    assert "Input validation failed" not in captured.err


def test_cli_generate_ep_summaries_scans_all_vendors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output_paths = [
        tmp_path / "data" / "ep_summaries" / "verisk" / "verisk_ep_summary.long.csv"
    ]
    calls: dict[str, object] = {}

    def convert_ep_summaries(data_root: Path) -> list[Path]:
        calls["data_root"] = data_root
        return output_paths

    monkeypatch.setattr(cli, "convert_ep_summaries", convert_ep_summaries)

    assert (
        cli.main(["generate-ep-summaries", "--data-root", str(tmp_path / "data")]) == 0
    )

    assert calls == {"data_root": tmp_path / "data"}
    summary = capsys.readouterr().out
    assert "EP summary conversion complete" in summary
    assert f"wrote: {output_paths[0]}" in summary


def test_cli_generate_ep_summaries_specific_vendor_and_csv(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data"
    output_path = data_root / "ep_summaries" / "verisk" / "verisk_ep_summary.long.csv"
    calls: dict[str, object] = {}

    def convert_ep_summary(
        input_csv: Path, vendor: str, *, output_csv: Path
    ) -> pl.DataFrame:
        calls["convert"] = (input_csv, vendor, output_csv)
        return pl.DataFrame()

    monkeypatch.setattr(cli, "convert_ep_summary", convert_ep_summary)

    assert (
        cli.main(
            [
                "generate-ep-summaries",
                "--data-root",
                str(data_root),
                "--vendor",
                "verisk",
                "--csv",
                "nested/verisk_clean.csv",
            ]
        )
        == 0
    )

    assert calls == {
        "convert": (
            data_root / "ep_summaries" / "verisk" / "nested" / "verisk_clean.csv",
            "verisk",
            output_path,
        )
    }
    assert f"wrote: {output_path}" in capsys.readouterr().out


def test_cli_generate_ep_summaries_requires_vendor_and_csv_together(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert cli.main(["generate-ep-summaries", "--vendor", "verisk"]) == 1

    assert "--vendor and --csv must be passed together" in capsys.readouterr().err


def test_success_summary_reports_output_paths_and_counts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "output"
    marts_dir = output_root / "marts"
    analysis_report = output_root / "analysis" / "ep_report.csv"
    staging_dir = output_root / "stages" / "staging"
    intermediate_dir = output_root / "stages" / "intermediate"
    log_file = output_root / "rollup.log"

    data_root.mkdir()
    marts_dir.mkdir(parents=True)
    analysis_report.parent.mkdir(parents=True)
    staging_dir.mkdir(parents=True)
    intermediate_dir.mkdir(parents=True)
    log_file.touch()
    analysis_report.touch()
    for parquet_path in (
        marts_dir / "combined.parquet",
        marts_dir / "wide.parquet",
        marts_dir / "dialsup.parquet",
        staging_dir / "staging-one.parquet",
        staging_dir / "staging-two.parquet",
        intermediate_dir / "intermediate.parquet",
    ):
        parquet_path.touch()

    cli.print_success_summary(
        rollup_result(data_root, output_root, ep_report_path=analysis_report),
        log_file,
    )

    summary = capsys.readouterr().out
    assert f"output root: {output_root} (exists)" in summary
    assert f"log file: {log_file} (exists)" in summary
    assert f"marts dir: {marts_dir} (exists, 3 parquet files)" in summary
    assert f"analysis report: {analysis_report} (exists)" in summary
    assert f"stage outputs: {output_root / 'stages'} (exists)" in summary
    assert f"staging: {staging_dir} (2 parquet files)" in summary
    assert f"intermediate: {intermediate_dir} (1 parquet file)" in summary
    assert "WARNING:" not in summary


def test_success_summary_warns_when_enabled_outputs_are_missing(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output_root = tmp_path / "output"
    analysis_report = output_root / "analysis" / "ep_report.csv"
    stage_dir = output_root / "stages"

    cli.print_success_summary(
        rollup_result(tmp_path / "data", output_root, ep_report_path=analysis_report),
        output_root / "rollup.log",
    )

    summary = capsys.readouterr().out
    assert f"analysis report: {analysis_report} (missing)" in summary
    assert f"stage outputs: {stage_dir} (missing)" in summary
    assert f"staging: {stage_dir / 'staging'} (0 parquet files)" in summary
    assert f"intermediate: {stage_dir / 'intermediate'} (0 parquet files)" in summary
    assert "WARNING: analysis report missing:" in summary
    assert (
        "WARNING: stage outputs incomplete: staging directory missing, intermediate directory missing"
        in summary
    )


def rollup_result(
    data_root: Path,
    output_root: Path,
    *,
    ep_report_path: Path | None,
    stage_dir: Path | None | object = ...,
    duckdb_file: Path | None = None,
) -> RollupRunResult:
    stage_path = output_root / "stages" if stage_dir is ... else stage_dir
    return RollupRunResult(
        data_root=data_root,
        output_root=output_root,
        outputs=RollupOutputPaths(
            mts_combined=output_root / "marts" / "combined.parquet",
            mts_wide=output_root / "marts" / "wide.parquet",
            mts_dialsup=output_root / "marts" / "dialsup.parquet",
            marts_dir=output_root / "marts",
            mart_files=(),
            stage_dir=stage_path,
            duckdb_file=duckdb_file,
        ),
        ep_report_path=ep_report_path,
    )
