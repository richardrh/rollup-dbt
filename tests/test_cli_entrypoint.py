from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import tomllib


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
