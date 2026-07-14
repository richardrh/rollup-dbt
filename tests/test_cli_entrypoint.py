from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import tomllib
from argparse import Namespace
from types import SimpleNamespace

from rollup import cli
from rollup.config import OutputConfig, RollupConfig


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_project_installs_rollup_console_script() -> None:
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert pyproject["project"]["scripts"]["rollup"] == "rollup.cli:main"


def test_rollup_cli_module_is_executable() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "rollup.cli", "--help"],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Local rollup test runner" in result.stdout


def test_run_command_accepts_output_root_after_subcommand(monkeypatch, tmp_path: Path) -> None:
    calls = []

    def run_command(args):
        calls.append(args)
        return 0

    monkeypatch.setattr(cli, "run_command", run_command)

    assert cli.main(["run", "--output-root", str(tmp_path), "--duckdb"]) == 0

    assert calls[0].output_root == tmp_path
    assert calls[0].duckdb is True


def test_run_command_accepts_no_duckdb_after_subcommand(monkeypatch, tmp_path: Path) -> None:
    calls = []

    def run_command(args):
        calls.append(args)
        return 0

    monkeypatch.setattr(cli, "run_command", run_command)

    assert cli.main(["run", "--output-root", str(tmp_path), "--no-duckdb"]) == 0

    assert calls[0].no_duckdb is True


def test_run_command_rejects_no_duckdb_with_duckdb_file(tmp_path: Path) -> None:
    try:
        cli.main(["run", "--no-duckdb", "--duckdb-file", str(tmp_path / "x.duckdb")])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected parser rejection")


def test_parser_rejects_removed_sql_commands_and_options() -> None:
    parser = cli.build_parser()

    for argv in (["sql-check"], ["test-sql"], ["run", "--push-sql"]):
        try:
            parser.parse_args(argv)
        except SystemExit as exc:
            assert exc.code == 2
        else:
            raise AssertionError(f"expected parser rejection for {argv}")


def test_help_does_not_list_removed_sql_commands() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "rollup.cli", "--help"],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "sql-check" not in result.stdout
    assert "test-sql" not in result.stdout


def test_run_command_invokes_current_run_rollup_api_once(monkeypatch, tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "output"
    calls: list[dict[str, object]] = []

    def run_rollup(data_root_arg: Path, output_root_arg: Path, **kwargs: object) -> SimpleNamespace:
        calls.append({"data_root": data_root_arg, "output_root": output_root_arg, **kwargs})
        return SimpleNamespace(ep_report_path=output_root / "analysis" / "ep_report.csv")

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
    assert len(calls) == 1
    call = calls[0]
    assert call["data_root"] == data_root
    assert call["output_root"] == output_root
    assert call["debug"] is False
    assert "validation_callback" not in call


def test_no_duckdb_override_disables_configured_export(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        cli,
        "load_config",
        lambda path: RollupConfig(outputs=OutputConfig(write_duckdb=True, duckdb_file="configured.duckdb")),
    )
    config = cli.override_config(
        Namespace(
            duckdb=False,
            no_duckdb=True,
            duckdb_file=None,
            log_format=None,
            config_path=tmp_path / "config.toml",
        )
    )

    assert config is not None
    assert config.outputs.write_duckdb is False
    assert config.outputs.duckdb_file is None
