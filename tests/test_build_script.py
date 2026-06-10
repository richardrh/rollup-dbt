from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


def load_build_script() -> ModuleType:
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "build.py"
    spec = importlib.util.spec_from_file_location("rollup_build_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_required_source_files_match_current_package() -> None:
    build_script = load_build_script()

    required_files = build_script.REQUIRED_SOURCE_FILES

    assert "sql.py" not in required_files
    assert required_files.count("pipeline.py") == 1
    assert "__main__.py" in required_files
    assert "config.py" in required_files
    assert "duckdb_export.py" in required_files
    assert build_script.check_source_structure() == 0


def test_build_binary_runs_pyinstaller_module(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    build_script = load_build_script()
    (tmp_path / "rollup.spec").write_text("# spec\n", encoding="utf-8")
    calls: list[dict[str, object]] = []

    def run(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
        calls.append({"command": command, "cwd": cwd})
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(build_script, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(build_script.subprocess, "run", run)

    assert build_script.build_binary() == 0
    assert calls == [
        {
            "command": [sys.executable, "-m", "PyInstaller", "-y", "rollup.spec"],
            "cwd": tmp_path,
        }
    ]


def test_package_no_version_prompt_skips_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    build_script = load_build_script()
    calls: list[str] = []

    def prompt_version(current_version: str) -> str:
        raise AssertionError(f"unexpected prompt for {current_version}")

    monkeypatch.setattr(build_script, "get_version", lambda: "0.12.0")
    monkeypatch.setattr(build_script, "prompt_version", prompt_version)
    monkeypatch.setattr(build_script, "check_dependencies", lambda: 0)
    monkeypatch.setattr(build_script, "build_package", lambda: calls.append("package") or 0)

    assert build_script.main(["package", "--no-version-prompt"]) == 0
    assert calls == ["package"]


def test_all_target_builds_package_then_binary_without_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    build_script = load_build_script()
    calls: list[str] = []

    monkeypatch.setattr(build_script, "get_version", lambda: "0.12.0")
    monkeypatch.setattr(
        build_script,
        "prompt_version",
        lambda current_version: pytest.fail(f"unexpected prompt for {current_version}"),
    )
    monkeypatch.setattr(build_script, "check_dependencies", lambda: calls.append("check") or 0)
    monkeypatch.setattr(build_script, "build_package", lambda: calls.append("package") or 0)
    monkeypatch.setattr(build_script, "build_binary", lambda: calls.append("binary") or 0)

    assert build_script.main(["all", "--no-version-prompt"]) == 0
    assert calls == ["check", "package", "binary"]


def test_unknown_target_returns_nonzero() -> None:
    build_script = load_build_script()

    assert build_script.main(["does-not-exist"]) == 2
