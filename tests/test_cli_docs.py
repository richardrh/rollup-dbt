from __future__ import annotations

from collections.abc import Sequence
import json
from pathlib import Path
import sys

from rollup import cli
from rollup import resources as rollup_resources


REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_docs_project(root: Path) -> None:
    docs_dir = root / "docs"
    docs_dir.mkdir()
    (docs_dir / "index.md").write_text("# Docs\n", encoding="utf-8")
    (root / "zensical.toml").write_text(
        "[project]\nsite_name = 'Test'\ndocs_dir = 'docs'\n",
        encoding="utf-8",
    )


def _write_docs_state(root: Path, *, pid: int, host: str, port: int) -> None:
    tmp_dir = root / ".tmp"
    tmp_dir.mkdir(exist_ok=True)
    (tmp_dir / "rollup-docs.pid").write_text(
        json.dumps({"pid": pid, "host": host, "port": port}) + "\n",
        encoding="utf-8",
    )


def _expected_source_docs_command(host: str, port: int) -> list[str]:
    return [
        sys.executable,
        "-m",
        "rollup.cli",
        "docs",
        "--host",
        host,
        "--port",
        str(port),
        "--foreground",
    ]


def _cmdline_bytes(args: Sequence[str]) -> bytes:
    return b"".join(arg.encode() + b"\0" for arg in args)


def _use_tmp_docs_project(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(rollup_resources, "resource_root", lambda: tmp_path)
    monkeypatch.setattr(rollup_resources, "is_frozen", lambda: False)


def test_docs_command_starts_background_server(monkeypatch, tmp_path, capsys) -> None:
    _write_docs_project(tmp_path)
    _use_tmp_docs_project(monkeypatch, tmp_path)
    popen_calls: list[dict[str, object]] = []

    class FakeProcess:
        pid = 4321

        def wait(self, *, timeout):
            raise cli.subprocess.TimeoutExpired("rollup docs", timeout)

    def fake_popen(command, *, stdout, stderr):
        popen_calls.append({"command": command, "stdout": stdout, "stderr": stderr})
        return FakeProcess()

    monkeypatch.setattr(cli.subprocess, "Popen", fake_popen)

    assert cli.docs_command(host="127.0.0.1", port=8000) == 0

    assert popen_calls == [
        {
            "command": _expected_source_docs_command("127.0.0.1", 8000),
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
    _write_docs_project(tmp_path)
    _write_docs_state(tmp_path, pid=9876, host="127.0.0.1", port=8000)
    _use_tmp_docs_project(monkeypatch, tmp_path)

    def fake_kill(pid: int, signal: int) -> None:
        assert pid == 9876
        assert signal == 0

    def unexpected_popen(*args, **kwargs):
        raise AssertionError("duplicate docs server should not be spawned")

    def fake_read_bytes(path: Path) -> bytes:
        assert str(path) == "/proc/9876/cmdline"
        return _cmdline_bytes(_expected_source_docs_command("127.0.0.1", 8000))

    monkeypatch.setattr(cli.os, "kill", fake_kill)
    monkeypatch.setattr(cli.Path, "read_bytes", fake_read_bytes)
    monkeypatch.setattr(cli.subprocess, "Popen", unexpected_popen)

    assert cli.docs_command(host="127.0.0.1", port=8000) == 0

    output = capsys.readouterr().out
    assert "Docs available at http://127.0.0.1:8000/" in output
    assert "PID 9876" in output
    assert "Logs: .tmp/rollup-docs.log" in output
    assert "Stop with: kill 9876" in output


def test_docs_command_ignores_stale_pid_and_starts_new_server(monkeypatch, tmp_path) -> None:
    _write_docs_project(tmp_path)
    _write_docs_state(tmp_path, pid=9876, host="127.0.0.1", port=8000)
    _use_tmp_docs_project(monkeypatch, tmp_path)
    popen_commands: list[list[str]] = []

    class FakeProcess:
        pid = 4321

        def wait(self, *, timeout):
            raise cli.subprocess.TimeoutExpired("rollup docs", timeout)

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

    assert popen_commands == [_expected_source_docs_command("127.0.0.1", 8000)]
    assert json.loads((tmp_path / ".tmp" / "rollup-docs.pid").read_text())["pid"] == 4321


def test_pid_is_alive_returns_false_for_generic_oserror(monkeypatch) -> None:
    def fake_kill(pid: int, signal: int) -> None:
        assert pid == 9876
        assert signal == 0
        raise OSError("invalid parameter")

    monkeypatch.setattr(cli.os, "kill", fake_kill)

    assert cli._pid_is_alive(9876) is False


def test_docs_command_does_not_reuse_non_docs_process_cmdline(monkeypatch, tmp_path) -> None:
    _write_docs_project(tmp_path)
    _write_docs_state(tmp_path, pid=9876, host="127.0.0.1", port=8000)
    _use_tmp_docs_project(monkeypatch, tmp_path)
    popen_commands: list[list[str]] = []

    class FakeProcess:
        pid = 4321

        def wait(self, *, timeout):
            raise cli.subprocess.TimeoutExpired("rollup docs", timeout)

    def fake_kill(pid: int, signal: int) -> None:
        assert pid == 9876
        assert signal == 0

    def fake_read_bytes(path: Path) -> bytes:
        assert str(path) == "/proc/9876/cmdline"
        return b"/usr/bin/python\0-c\0import time; time.sleep(999)\0"

    def fake_popen(command, *, stdout, stderr):
        popen_commands.append(command)
        return FakeProcess()

    monkeypatch.setattr(cli.os, "kill", fake_kill)
    monkeypatch.setattr(cli.Path, "read_bytes", fake_read_bytes)
    monkeypatch.setattr(cli.subprocess, "Popen", fake_popen)

    assert cli.docs_command(host="127.0.0.1", port=8000) == 0

    assert popen_commands == [_expected_source_docs_command("127.0.0.1", 8000)]
    assert json.loads((tmp_path / ".tmp" / "rollup-docs.pid").read_text()) == {
        "pid": 4321,
        "host": "127.0.0.1",
        "port": 8000,
    }


def test_docs_command_does_not_reuse_mismatched_host_port_state(monkeypatch, tmp_path) -> None:
    _write_docs_project(tmp_path)
    _write_docs_state(tmp_path, pid=9876, host="127.0.0.1", port=8000)
    _use_tmp_docs_project(monkeypatch, tmp_path)
    popen_commands: list[list[str]] = []

    class FakeProcess:
        pid = 2468

        def wait(self, *, timeout):
            raise cli.subprocess.TimeoutExpired("rollup docs", timeout)

    def unexpected_kill(*args, **kwargs):
        raise AssertionError("mismatched host/port state should not be treated as reusable")

    def fake_popen(command, *, stdout, stderr):
        popen_commands.append(command)
        return FakeProcess()

    monkeypatch.setattr(cli.os, "kill", unexpected_kill)
    monkeypatch.setattr(cli.subprocess, "Popen", fake_popen)

    assert cli.docs_command(host="0.0.0.0", port=9000) == 0

    assert popen_commands == [_expected_source_docs_command("0.0.0.0", 9000)]
    assert json.loads((tmp_path / ".tmp" / "rollup-docs.pid").read_text()) == {
        "pid": 2468,
        "host": "0.0.0.0",
        "port": 9000,
    }


def test_docs_command_reports_immediate_startup_failure(monkeypatch, tmp_path, capsys) -> None:
    _write_docs_project(tmp_path)
    _use_tmp_docs_project(monkeypatch, tmp_path)

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


def test_docs_command_foreground_uses_in_process_zensical(monkeypatch, tmp_path, capsys) -> None:
    _write_docs_project(tmp_path)
    _use_tmp_docs_project(monkeypatch, tmp_path)
    runner_calls: list[list[str]] = []

    def runner(args: Sequence[str]) -> int:
        runner_calls.append(list(args))
        return 7

    def unexpected_popen(*args, **kwargs):
        raise AssertionError("foreground docs should not spawn a background process")

    monkeypatch.setattr(cli.subprocess, "Popen", unexpected_popen)

    assert cli.docs_command(
        host="0.0.0.0",
        port=9000,
        foreground=True,
        zensical_runner=runner,
    ) == 7

    assert runner_calls == [
        [
            "serve",
            "--config-file",
            str(tmp_path / "zensical.toml"),
            "--dev-addr",
            "0.0.0.0:9000",
        ]
    ]
    assert "Docs available at http://0.0.0.0:9000/" in capsys.readouterr().out


def test_docs_command_reports_missing_docs_dir(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(rollup_resources, "resource_root", lambda: tmp_path)

    assert cli.docs_command() == 1

    output = capsys.readouterr()
    assert output.out == ""
    assert f"Documentation source directory was not found: {tmp_path / 'docs'}" in output.err


def test_docs_command_reports_background_start_failure(monkeypatch, tmp_path, capsys) -> None:
    _write_docs_project(tmp_path)
    _use_tmp_docs_project(monkeypatch, tmp_path)

    monkeypatch.setattr(
        cli.subprocess,
        "Popen",
        lambda *args, **kwargs: (_ for _ in ()).throw(FileNotFoundError()),
    )

    assert cli.docs_command() == 1

    assert "Could not start the docs server process" in capsys.readouterr().err


def test_docs_foreground_flag_is_wired_through_main(monkeypatch, tmp_path) -> None:
    _write_docs_project(tmp_path)
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
