from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import tomllib
from argparse import Namespace

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
