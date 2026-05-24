from __future__ import annotations

from collections.abc import Sequence
import json
from pathlib import Path
import subprocess
import sys

from rollup import cli
from rollup import resources as rollup_resources


def _write_docs_project(root: Path) -> None:
    (root / "docs").mkdir(parents=True)
    (root / "docs" / "index.md").write_text("# Docs\n", encoding="utf-8")
    (root / "zensical.toml").write_text(
        "[project]\nsite_name = 'Test'\ndocs_dir = 'docs'\n",
        encoding="utf-8",
    )


def test_docs_command_uses_resolved_resources_and_in_process_zensical(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_docs_project(tmp_path)
    monkeypatch.setattr(rollup_resources, "resource_root", lambda: tmp_path)

    calls: list[tuple[list[str], Path]] = []

    def runner(args: Sequence[str]) -> int:
        calls.append((list(args), Path.cwd()))
        return 0

    exit_code = cli.docs_command(
        host="0.0.0.0",
        port=9000,
        foreground=True,
        zensical_runner=runner,
    )

    assert exit_code == 0
    assert calls == [
        (
            [
                "serve",
                "--config-file",
                str(tmp_path / "zensical.toml"),
                "--dev-addr",
                "0.0.0.0:9000",
            ],
            tmp_path,
        )
    ]
    assert "Docs available at http://0.0.0.0:9000/" in capsys.readouterr().out


def test_docs_command_copies_bundled_docs_to_writable_runtime_project(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bundle_root = tmp_path / "_internal"
    _write_docs_project(bundle_root)
    monkeypatch.setattr(rollup_resources, "resource_root", lambda: bundle_root)
    monkeypatch.setattr(rollup_resources, "is_frozen", lambda: True)

    calls: list[tuple[list[str], Path]] = []

    def runner(args: Sequence[str]) -> int:
        runtime_root = Path.cwd()
        calls.append((list(args), runtime_root))
        assert runtime_root != bundle_root
        assert runtime_root.name.startswith("rollup-docs-")
        assert (runtime_root / "docs" / "index.md").read_text(encoding="utf-8") == "# Docs\n"
        assert (runtime_root / "zensical.toml").is_file()
        return 0

    exit_code = cli.docs_command(foreground=True, zensical_runner=runner)

    assert exit_code == 0
    assert calls
    args, runtime_root = calls[0]
    assert args == [
        "serve",
        "--config-file",
        str(runtime_root / "zensical.toml"),
        "--dev-addr",
        "127.0.0.1:8000",
    ]


def test_docs_command_returns_error_when_docs_are_missing(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(rollup_resources, "resource_root", lambda: tmp_path)

    exit_code = cli.docs_command()

    captured = capsys.readouterr()
    assert exit_code == 1
    assert f"Documentation source directory was not found: {tmp_path / 'docs'}" in captured.err


def test_docs_command_returns_error_when_zensical_config_is_missing(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    (tmp_path / "docs").mkdir()
    monkeypatch.setattr(rollup_resources, "resource_root", lambda: tmp_path)

    exit_code = cli.docs_command()

    captured = capsys.readouterr()
    assert exit_code == 1
    assert f"Zensical configuration file was not found: {tmp_path / 'zensical.toml'}" in captured.err


def test_docs_command_backgrounds_current_module_and_records_state(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_docs_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(rollup_resources, "resource_root", lambda: tmp_path)
    monkeypatch.setattr(rollup_resources, "is_frozen", lambda: False)

    calls: list[tuple[list[str], bool, bool]] = []

    class RunningProcess:
        pid = 4321

        def wait(self, timeout: float) -> int:
            raise subprocess.TimeoutExpired(cmd="rollup docs", timeout=timeout)

    def process_factory(command: list[str], *, stdout, stderr) -> RunningProcess:
        calls.append((command, stdout.closed, stderr.closed))
        return RunningProcess()

    exit_code = cli.docs_command(host="0.0.0.0", port=9000, process_factory=process_factory)

    assert exit_code == 0
    assert calls == [
        (
            [
                sys.executable,
                "-m",
                "rollup.cli",
                "docs",
                "--host",
                "0.0.0.0",
                "--port",
                "9000",
                "--foreground",
            ],
            False,
            False,
        )
    ]
    assert json.loads((tmp_path / ".tmp" / "rollup-docs.pid").read_text()) == {
        "pid": 4321,
        "host": "0.0.0.0",
        "port": 9000,
    }
    captured = capsys.readouterr().out
    assert "Docs available at http://0.0.0.0:9000/" in captured
    assert "Docs server running in background with PID 4321" in captured
    assert "Logs: .tmp/rollup-docs.log" in captured


def test_docs_command_reuses_matching_live_background_server(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_docs_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(rollup_resources, "resource_root", lambda: tmp_path)
    monkeypatch.setattr(cli, "_pid_is_alive", lambda pid: True)
    monkeypatch.setattr(cli, "_process_matches_docs_command", lambda pid, command: True)
    (tmp_path / ".tmp").mkdir()
    (tmp_path / ".tmp" / "rollup-docs.pid").write_text(
        json.dumps({"pid": 2468, "host": "127.0.0.1", "port": 8000}) + "\n",
        encoding="utf-8",
    )

    def process_factory(*args, **kwargs):
        raise AssertionError("process should not be started for a live matching docs server")

    exit_code = cli.docs_command(process_factory=process_factory)

    assert exit_code == 0
    captured = capsys.readouterr().out
    assert "Docs available at http://127.0.0.1:8000/" in captured
    assert "Docs server running in background with PID 2468" in captured


def test_docs_server_command_backgrounds_frozen_executable(monkeypatch) -> None:
    monkeypatch.setattr(rollup_resources, "is_frozen", lambda: True)
    monkeypatch.setattr(sys, "executable", "/opt/rollup/rollup")

    assert cli._docs_server_command("127.0.0.1", 8000) == [
        "/opt/rollup/rollup",
        "docs",
        "--host",
        "127.0.0.1",
        "--port",
        "8000",
        "--foreground",
    ]
