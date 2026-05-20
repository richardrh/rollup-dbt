from __future__ import annotations

import json
from pathlib import Path

from rollup import cli


REPO_ROOT = Path(__file__).resolve().parents[1]


def _create_docs_dir(root: Path) -> None:
    docs_dir = root / "docs"
    docs_dir.mkdir()
    (docs_dir / "index.md").write_text("# Docs\n")


def _write_docs_state(root: Path, *, pid: int, host: str, port: int) -> None:
    tmp_dir = root / ".tmp"
    tmp_dir.mkdir(exist_ok=True)
    (tmp_dir / "rollup-docs.pid").write_text(
        json.dumps({"pid": pid, "host": host, "port": port}) + "\n"
    )


def test_docs_command_starts_background_server(monkeypatch, tmp_path, capsys) -> None:
    _create_docs_dir(tmp_path)
    monkeypatch.chdir(tmp_path)
    popen_calls: list[dict[str, object]] = []

    class FakeProcess:
        pid = 4321

        def wait(self, *, timeout):
            raise cli.subprocess.TimeoutExpired("zensical", timeout)

    def fake_popen(command, *, stdout, stderr):
        popen_calls.append({"command": command, "stdout": stdout, "stderr": stderr})
        return FakeProcess()

    monkeypatch.setattr(cli.subprocess, "Popen", fake_popen)

    assert cli.docs_command(host="127.0.0.1", port=8000) == 0

    assert popen_calls == [
        {
            "command": [
                "zensical",
                "serve",
                "--config-file",
                "zensical.toml",
                "--dev-addr",
                "127.0.0.1:8000",
            ],
            "stdout": popen_calls[0]["stdout"],
            "stderr": popen_calls[0]["stdout"],
        }
    ]
    assert (tmp_path / ".tmp" / "rollup-docs.log").is_file()
    assert json.loads((tmp_path / ".tmp" / "rollup-docs.pid").read_text()) == {
        "pid": 4321,
        "host": "127.0.0.1",
        "port": 8000,
    }

    output = capsys.readouterr().out
    assert "Docs available at http://127.0.0.1:8000/" in output
    assert "PID 4321" in output
    assert "Logs: .tmp/rollup-docs.log" in output
    assert "Stop with: kill 4321" in output


def test_docs_command_reuses_existing_live_background_server(monkeypatch, tmp_path, capsys) -> None:
    _create_docs_dir(tmp_path)
    _write_docs_state(tmp_path, pid=9876, host="127.0.0.1", port=8000)
    monkeypatch.chdir(tmp_path)

    def fake_kill(pid: int, signal: int) -> None:
        assert pid == 9876
        assert signal == 0

    def unexpected_popen(*args, **kwargs):
        raise AssertionError("duplicate docs server should not be spawned")

    monkeypatch.setattr(cli.os, "kill", fake_kill)
    monkeypatch.setattr(cli.subprocess, "Popen", unexpected_popen)

    assert cli.docs_command(host="127.0.0.1", port=8000) == 0

    output = capsys.readouterr().out
    assert "Docs available at http://127.0.0.1:8000/" in output
    assert "PID 9876" in output
    assert "Logs: .tmp/rollup-docs.log" in output
    assert "Stop with: kill 9876" in output


def test_docs_command_ignores_stale_pid_and_starts_new_server(monkeypatch, tmp_path) -> None:
    _create_docs_dir(tmp_path)
    _write_docs_state(tmp_path, pid=9876, host="127.0.0.1", port=8000)
    monkeypatch.chdir(tmp_path)
    popen_commands: list[list[str]] = []

    class FakeProcess:
        pid = 4321

        def wait(self, *, timeout):
            raise cli.subprocess.TimeoutExpired("zensical", timeout)

    def fake_kill(pid: int, signal: int) -> None:
        assert pid == 9876
        assert signal == 0
        raise ProcessLookupError

    def fake_popen(command, *, stdout, stderr):
        popen_commands.append(command)
        return FakeProcess()

    monkeypatch.setattr(cli.os, "kill", fake_kill)
    monkeypatch.setattr(cli.subprocess, "Popen", fake_popen)

    assert cli.docs_command(host="127.0.0.1", port=8000) == 0

    assert popen_commands == [
        [
            "zensical",
            "serve",
            "--config-file",
            "zensical.toml",
            "--dev-addr",
            "127.0.0.1:8000",
        ]
    ]
    assert json.loads((tmp_path / ".tmp" / "rollup-docs.pid").read_text())["pid"] == 4321


def test_docs_command_does_not_reuse_mismatched_host_port_state(monkeypatch, tmp_path) -> None:
    _create_docs_dir(tmp_path)
    _write_docs_state(tmp_path, pid=9876, host="127.0.0.1", port=8000)
    monkeypatch.chdir(tmp_path)
    popen_commands: list[list[str]] = []

    class FakeProcess:
        pid = 2468

        def wait(self, *, timeout):
            raise cli.subprocess.TimeoutExpired("zensical", timeout)

    def unexpected_kill(*args, **kwargs):
        raise AssertionError("mismatched host/port state should not be treated as reusable")

    def fake_popen(command, *, stdout, stderr):
        popen_commands.append(command)
        return FakeProcess()

    monkeypatch.setattr(cli.os, "kill", unexpected_kill)
    monkeypatch.setattr(cli.subprocess, "Popen", fake_popen)

    assert cli.docs_command(host="0.0.0.0", port=9000) == 0

    assert popen_commands == [
        [
            "zensical",
            "serve",
            "--config-file",
            "zensical.toml",
            "--dev-addr",
            "0.0.0.0:9000",
        ]
    ]
    assert json.loads((tmp_path / ".tmp" / "rollup-docs.pid").read_text()) == {
        "pid": 2468,
        "host": "0.0.0.0",
        "port": 9000,
    }


def test_docs_command_reports_immediate_startup_failure(monkeypatch, tmp_path, capsys) -> None:
    _create_docs_dir(tmp_path)
    monkeypatch.chdir(tmp_path)

    class FailedProcess:
        pid = 4321

        def wait(self, *, timeout):
            return 2

    monkeypatch.setattr(cli.subprocess, "Popen", lambda *args, **kwargs: FailedProcess())

    assert cli.docs_command(host="127.0.0.1", port=8000) == 2

    output = capsys.readouterr()
    assert output.out == ""
    assert "Docs server exited immediately with status 2" in output.err
    assert "See logs: .tmp/rollup-docs.log" in output.err
    assert not (tmp_path / ".tmp" / "rollup-docs.pid").exists()


def test_docs_command_foreground_uses_blocking_call(monkeypatch, tmp_path, capsys) -> None:
    _create_docs_dir(tmp_path)
    monkeypatch.chdir(tmp_path)
    call_commands: list[list[str]] = []

    def fake_call(command):
        call_commands.append(command)
        return 7

    def unexpected_popen(*args, **kwargs):
        raise AssertionError("foreground docs should use subprocess.call")

    monkeypatch.setattr(cli.subprocess, "call", fake_call)
    monkeypatch.setattr(cli.subprocess, "Popen", unexpected_popen)

    assert cli.docs_command(host="0.0.0.0", port=9000, foreground=True) == 7

    assert call_commands == [
        [
            "zensical",
            "serve",
            "--config-file",
            "zensical.toml",
            "--dev-addr",
            "0.0.0.0:9000",
        ]
    ]
    assert "Docs available at http://0.0.0.0:9000/" in capsys.readouterr().out


def test_docs_command_reports_missing_docs_dir(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)

    assert cli.docs_command() == 1

    output = capsys.readouterr()
    assert output.out == ""
    assert "Documentation source directory 'docs/' was not found" in output.err


def test_docs_command_reports_missing_zensical(monkeypatch, tmp_path, capsys) -> None:
    _create_docs_dir(tmp_path)
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr(
        cli.subprocess,
        "Popen",
        lambda *args, **kwargs: (_ for _ in ()).throw(FileNotFoundError()),
    )

    assert cli.docs_command() == 1

    assert "Could not find the 'zensical' executable" in capsys.readouterr().err


def test_docs_foreground_flag_is_wired_through_main(monkeypatch, tmp_path) -> None:
    _create_docs_dir(tmp_path)
    monkeypatch.chdir(tmp_path)

    foreground_values: list[bool] = []

    def fake_docs_command(*, host: str, port: int, foreground: bool) -> int:
        assert host == "127.0.0.1"
        assert port == 8000
        foreground_values.append(foreground)
        return 0

    monkeypatch.setattr(cli, "docs_command", fake_docs_command)

    assert cli.main(["docs", "--foreground"]) == 0
    assert foreground_values == [True]


def test_docs_runtime_state_directory_is_gitignored() -> None:
    assert "/.tmp/" in (REPO_ROOT / ".gitignore").read_text().splitlines()
