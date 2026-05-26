from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from rollup import cli
from rollup.sql import SqlCheckResult, SqlConfig


def test_parser_accepts_run_push_sql_config_after_subcommand() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(["run", "--push-sql", "--config", "rollup.local.toml"])

    assert args.command == "run"
    assert args.push_sql is True
    assert args.config == Path("rollup.local.toml")


def test_parser_accepts_global_config_before_sql_check() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(["--config", "custom.toml", "sql-check"])

    assert args.command == "sql-check"
    assert args.config == Path("custom.toml")


def test_parser_accepts_test_sql_alias_with_config() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(["test-sql", "--config", "rollup.local.toml"])

    assert args.command == "test-sql"
    assert args.config == Path("rollup.local.toml")


def test_parser_keeps_sql_ep_summary_and_background_docs_options() -> None:
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
    docs_args = parser.parse_args(["docs", "--foreground"])

    assert sql_args.command == "sql-check"
    assert sql_args.config == Path("sql.toml")
    assert ep_args.command == "generate-ep-summaries"
    assert ep_args.vendor == "verisk"
    assert ep_args.csv == Path("selected.csv")
    assert ep_args.yes is True
    assert docs_args.command == "docs"
    assert docs_args.foreground is True


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


def test_run_without_push_sql_writes_analysis_but_does_not_push(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "output"
    expected_inputs = object()
    reports = SimpleNamespace(inputs=expected_inputs)
    events: list[str] = []

    monkeypatch.setattr(
        cli,
        "collect_validation_reports",
        lambda data_root_arg: events.append("collect_validation") or reports,
    )
    monkeypatch.setattr(
        cli,
        "print_validation_reports",
        lambda reports_arg: events.append("print_validation"),
    )
    monkeypatch.setattr(cli, "validation_exit_code", lambda reports_arg: 0)

    def run_pipeline(
        data_root_arg: Path,
        *,
        output_root: Path,
        debug: bool,
        validation_inputs: object,
    ) -> None:
        assert data_root_arg == data_root
        assert output_root == output_root_path
        assert debug is False
        assert validation_inputs is expected_inputs
        events.append("run_pipeline")

    output_root_path = output_root
    monkeypatch.setattr(cli, "run", run_pipeline)
    monkeypatch.setattr(
        cli,
        "write_ep_report",
        lambda output_root_arg: events.append("write_ep_report")
        or output_root_arg / "analysis" / "ep_report.csv",
    )
    monkeypatch.setattr(
        cli,
        "push_mart_parquets_to_sql",
        lambda output_root, sql_config: pytest.fail("run should not push SQL"),
    )

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
        "collect_validation",
        "print_validation",
        "run_pipeline",
        "write_ep_report",
    ]


def test_run_push_sql_happens_after_pipeline_and_analysis(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "output"
    config_path = tmp_path / "rollup.local.toml"
    expected_inputs = object()
    reports = SimpleNamespace(inputs=expected_inputs)
    events: list[str] = []

    monkeypatch.setattr(
        cli,
        "collect_validation_reports",
        lambda data_root_arg: events.append("collect_validation") or reports,
    )
    monkeypatch.setattr(
        cli,
        "print_validation_reports",
        lambda reports_arg: events.append("print_validation"),
    )
    monkeypatch.setattr(cli, "validation_exit_code", lambda reports_arg: 0)

    def run_pipeline(
        data_root_arg: Path,
        *,
        output_root: Path,
        debug: bool,
        validation_inputs: object,
    ) -> None:
        assert data_root_arg == data_root
        assert output_root == output_root_path
        assert debug is False
        assert validation_inputs is expected_inputs
        events.append("run_pipeline")

    output_root_path = output_root
    monkeypatch.setattr(cli, "run", run_pipeline)

    monkeypatch.setattr(
        cli,
        "write_ep_report",
        lambda output_root_arg: events.append("write_ep_report")
        or output_root_arg / "analysis" / "ep_report.csv",
    )

    sql_config = SqlConfig(connection_string="mssql+pyodbc://server/database")
    monkeypatch.setattr(
        cli,
        "require_working_sql_config",
        lambda config_path_arg: events.append("check_sql") or sql_config,
    )

    def push_sql(output_root_arg: Path, sql_config_arg: SqlConfig) -> list[str]:
        assert output_root_arg == output_root_path
        assert sql_config_arg is sql_config
        events.append("push_sql")
        return ["dbo.HiscoAIR_202601_main"]

    monkeypatch.setattr(cli, "push_mart_parquets_to_sql", push_sql)

    exit_code = cli.main(
        [
            "--data-root",
            str(data_root),
            "--output-root",
            str(output_root),
            "run",
            "--push-sql",
            "--config",
            str(config_path),
        ]
    )

    assert exit_code == 0
    assert events == [
        "collect_validation",
        "print_validation",
        "run_pipeline",
        "write_ep_report",
        "check_sql",
        "push_sql",
    ]


def test_run_push_sql_failure_is_user_friendly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    reports = SimpleNamespace(inputs=object())
    monkeypatch.setattr(cli, "collect_validation_reports", lambda data_root: reports)
    monkeypatch.setattr(cli, "print_validation_reports", lambda reports: None)
    monkeypatch.setattr(cli, "validation_exit_code", lambda reports: 0)
    monkeypatch.setattr(cli, "run", lambda *args, **kwargs: None)
    events: list[str] = []
    monkeypatch.setattr(
        cli,
        "write_ep_report",
        lambda output_root: events.append("write_ep_report")
        or output_root / "analysis" / "ep_report.csv",
    )
    monkeypatch.setattr(
        cli,
        "require_working_sql_config",
        lambda config_path: SqlConfig(connection_string="mssql+pyodbc://server/database"),
    )

    def push_failure(output_root: Path, sql_config: SqlConfig) -> list[str]:
        raise RuntimeError("write failed")

    monkeypatch.setattr(cli, "push_mart_parquets_to_sql", push_failure)

    exit_code = cli.main(
        [
            "--output-root",
            str(tmp_path / "output"),
            "run",
            "--push-sql",
        ]
    )

    assert exit_code == 1
    assert events == ["write_ep_report"]
    assert "Failed to push marts to SQL Server: write failed" in capsys.readouterr().err
