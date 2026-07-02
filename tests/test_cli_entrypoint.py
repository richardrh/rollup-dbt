from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import tomllib

from rollup import cli


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
