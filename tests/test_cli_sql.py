from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from rollup import cli
from rollup.sql import SqlCheckResult


def test_parser_rejects_run_push_sql_option() -> None:
    parser = cli.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["run", "--push-sql"])


def test_parser_accepts_global_config_before_sql_check() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(["--config", "custom.toml", "sql-check"])

    assert args.command == "sql-check"
    assert args.config == Path("custom.toml")


def test_parser_accepts_global_log_file() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(["--log-file", "output/run.log", "run"])

    assert args.command == "run"
    assert args.log_file == Path("output/run.log")


def test_configure_logging_writes_to_log_file(tmp_path: Path) -> None:
    log_file = tmp_path / "logs" / "run.log"

    cli.configure_logging("INFO", log_file=log_file)
    logging.getLogger("rollup.test").info("expected log line")

    assert log_file.is_file()
    assert "expected log line" in log_file.read_text(encoding="utf-8")


def test_parser_accepts_test_sql_alias_with_config() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(["test-sql", "--config", "rollup.local.toml"])

    assert args.command == "test-sql"
    assert args.config == Path("rollup.local.toml")


def test_parser_keeps_sql_ep_summary_and_docs_options() -> None:
    parser = cli.build_parser()

    sql_args = parser.parse_args(["sql-check", "--config", "sql.toml"])
    ep_args = parser.parse_args(
        [
            "generate-ep-summaries",
            "--vendor",
            "verisk",
            "--csv",
            "selected.csv",
            "--yes",
        ]
    )
    docs_args = parser.parse_args(["docs", "--port", "4322"])

    assert sql_args.command == "sql-check"
    assert sql_args.config == Path("sql.toml")
    assert ep_args.command == "generate-ep-summaries"
    assert ep_args.vendor == "verisk"
    assert ep_args.csv == Path("selected.csv")
    assert ep_args.yes is True
    assert docs_args.command == "docs"
    assert docs_args.port == 4322


def test_sql_check_command_returns_success_only_on_ok(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "check_sql_connection",
        lambda config_path: SqlCheckResult("OK", "connected"),
    )
    assert cli.sql_check_command(Path("rollup.local.toml")) == 0
    assert "SQL check OK" in capsys.readouterr().out

    monkeypatch.setattr(
        cli,
        "check_sql_connection",
        lambda config_path: SqlCheckResult("SKIPPED", "not configured"),
    )
    assert cli.sql_check_command(Path("rollup.local.toml")) == 1

    monkeypatch.setattr(
        cli,
        "check_sql_connection",
        lambda config_path: SqlCheckResult("FAIL", "bad connection"),
    )
    assert cli.sql_check_command(Path("rollup.local.toml")) == 1


def test_run_command_writes_analysis_without_sql_push(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "output"
    events: list[str] = []

    monkeypatch.setattr(
        cli,
        "print_validation_reports",
        lambda reports_arg: events.append("print_validation"),
    )

    def run_rollup(
        data_root_arg: Path,
        *,
        output_root: Path,
        debug: bool,
        validation_callback,
    ) -> SimpleNamespace:
        assert data_root_arg == data_root
        assert output_root == output_root_path
        assert debug is False
        validation_callback(SimpleNamespace())
        events.append("run_rollup")
        return SimpleNamespace(ep_report_path=output_root / "analysis" / "ep_report.csv")

    output_root_path = output_root
    monkeypatch.setattr(cli, "run_rollup", run_rollup)
    exit_code = cli.main(
        [
            "--data-root",
            str(data_root),
            "--output-root",
            str(output_root),
            "run",
        ]
    )

    assert exit_code == 0
    assert events == [
        "print_validation",
        "run_rollup",
    ]
