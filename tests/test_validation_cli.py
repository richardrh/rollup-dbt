from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from rollup import cli
from rollup.columns import Col
from rollup.validation import ValidationInputs


def _cli_reports(
    *, valid: bool = True, coverage_error: bool = False
) -> cli.ValidationReports:
    coverage_report = (
        pl.DataFrame({"severity": ["error"], "valid": [False]})
        if coverage_error
        else pl.DataFrame(schema={"severity": pl.String, "valid": pl.Boolean})
    )
    return cli.ValidationReports(
        data_root=Path("data"),
        is_valid=valid and not coverage_error,
        coverage_report=coverage_report,
        input_ylt_aal_report=pl.DataFrame(
            {Col.vendor: ["verisk"], "raw_aal": [0.0001]}
        ),
    )


def test_run_parser_write_analysis_defaults_true() -> None:
    args = cli.build_parser().parse_args(["run"])

    assert args.write_analysis is True


def test_validate_command_reports_missing_input_without_traceback(
    capsys, tmp_path
) -> None:
    report_dir = tmp_path / "reports"

    assert cli.validate_command(tmp_path / "missing-data", report_dir=report_dir) == 1

    captured = capsys.readouterr()
    assert "Validation failed:" in captured.err
    assert "Traceback" not in captured.err
    assert not report_dir.exists()


def test_validate_data_root_global_and_subcommand_placements_reach_validator(
    monkeypatch, tmp_path
) -> None:
    calls: list[Path] = []

    def fake_validate(data_root, *, report_dir=None):
        calls.append(data_root)
        return 0

    monkeypatch.setattr(cli, "validate_command", fake_validate)

    global_root = tmp_path / "global"
    subcommand_root = tmp_path / "subcommand"
    assert cli.main(["--data-root", str(global_root), "validate"]) == 0
    assert cli.main(["validate", "--data-root", str(subcommand_root)]) == 0

    assert calls == [global_root, subcommand_root]


def test_validate_global_data_root_console_script_style_reaches_validator(
    monkeypatch, tmp_path
) -> None:
    calls: list[Path] = []

    def fake_validate(data_root, *, report_dir=None):
        calls.append(data_root)
        return 0

    monkeypatch.setattr(cli, "validate_command", fake_validate)
    data_root = tmp_path / "console-data"
    monkeypatch.setattr(
        "sys.argv", ["rollup", "--data-root", str(data_root), "validate"]
    )

    assert cli.main() == 0
    assert calls == [data_root]


def test_validate_command_returns_nonzero_for_coverage_errors(monkeypatch) -> None:
    monkeypatch.setattr(
        cli,
        "collect_validation_reports",
        lambda data_root: _cli_reports(coverage_error=True),
    )

    assert cli.validate_command("data") == 1


def test_validate_parser_accepts_report_dir_only_on_validate_command() -> None:
    args = cli.build_parser().parse_args(
        ["validate", "--report-dir", "output/validation"]
    )

    assert args.command == "validate"
    assert args.report_dir == Path("output/validation")


def test_validate_command_is_quiet_by_default(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli, "collect_validation_reports", lambda data_root: _cli_reports()
    )

    assert cli.validate_command("data") == 0
    assert capsys.readouterr().out == ""


def test_collect_validation_reports_uses_precomputed_coverage_report(
    monkeypatch,
) -> None:
    coverage_report = pl.DataFrame(schema={"severity": pl.String})
    inputs = ValidationInputs(
        seeds={},
        ylts={},
        ep_summaries=pl.DataFrame().lazy(),
        coverage_report=coverage_report,
    )
    monkeypatch.setattr(cli.validation, "inspect_inputs", lambda data_root: inputs)
    monkeypatch.setattr(
        cli.validation, "input_ylt_aal_by_lob_peril_summary", lambda _: pl.DataFrame()
    )

    def fail_recompute(*args, **kwargs):
        raise AssertionError("coverage report was recomputed")

    monkeypatch.setattr(
        cli, "modelled_dimension_coverage_report", fail_recompute, raising=False
    )

    reports = cli.collect_validation_reports("data")

    assert reports.coverage_report is coverage_report


def test_validate_command_writes_csv_reports_without_console_chatter(
    monkeypatch, capsys, tmp_path
) -> None:
    monkeypatch.setattr(
        cli, "collect_validation_reports", lambda data_root: _cli_reports()
    )
    report_dir = tmp_path / "validation"

    assert cli.validate_command("data", report_dir=report_dir) == 0

    expected_files = {
        "modelled_lob_peril_anti_join_report.csv",
        "input_ylt_aal_by_lob_peril_summary.csv",
    }
    assert {path.name for path in report_dir.iterdir()} == expected_files
    for filename in expected_files:
        assert (report_dir / filename).read_text(encoding="utf-8")
    assert capsys.readouterr().out == ""


def test_validate_command_report_dir_preserves_validation_exit_code(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.setattr(
        cli,
        "collect_validation_reports",
        lambda data_root: _cli_reports(coverage_error=True),
    )

    report_dir = tmp_path / "validation"
    assert cli.validate_command("data", report_dir=report_dir) == 1
    assert (report_dir / "modelled_lob_peril_anti_join_report.csv").is_file()


def test_validate_command_returns_nonzero_when_report_writing_fails(
    monkeypatch, capsys, tmp_path
) -> None:
    monkeypatch.setattr(
        cli, "collect_validation_reports", lambda data_root: _cli_reports()
    )

    def fail_write(*args, **kwargs):
        raise OSError("permission denied")

    monkeypatch.setattr(cli, "write_validation_csv_reports", fail_write)

    assert cli.validate_command("data", report_dir=tmp_path / "validation") == 1
    captured = capsys.readouterr()
    assert "Failed to write validation CSV reports" in captured.err
    assert "permission denied" in captured.err


def test_validation_report_writes_preserve_existing_reports_when_second_write_fails(
    tmp_path, monkeypatch
) -> None:
    report_dir = tmp_path / "validation"
    report_dir.mkdir()
    coverage_path = report_dir / "modelled_lob_peril_anti_join_report.csv"
    aal_path = report_dir / "input_ylt_aal_by_lob_peril_summary.csv"
    coverage_path.write_text("old coverage\n", encoding="utf-8")
    aal_path.write_text("old aal\n", encoding="utf-8")
    original_write_csv = pl.DataFrame.write_csv
    writes = 0

    def fail_second_write(self, path, *args, **kwargs):
        nonlocal writes
        writes += 1
        if writes == 2:
            raise OSError("write failed")
        return original_write_csv(self, path, *args, **kwargs)

    monkeypatch.setattr(pl.DataFrame, "write_csv", fail_second_write)

    with pytest.raises(OSError, match="write failed"):
        cli.write_validation_csv_reports(_cli_reports(), report_dir)

    assert coverage_path.read_text(encoding="utf-8") == "old coverage\n"
    assert aal_path.read_text(encoding="utf-8") == "old aal\n"
    assert list(report_dir.glob("rollup-validation-*")) == []
