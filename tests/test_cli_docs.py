from __future__ import annotations

from pathlib import Path

from rollup import cli


def _create_docs_dir(root: Path) -> None:
    docs_dir = root / "docs"
    docs_dir.mkdir()
    (docs_dir / "index.md").write_text("# Docs\n")


def test_docs_command_starts_background_server(monkeypatch, tmp_path, capsys) -> None:
    _create_docs_dir(tmp_path)
    monkeypatch.chdir(tmp_path)
    popen_calls: list[dict[str, object]] = []

    class FakeProcess:
        pid = 4321

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
    assert (tmp_path / ".tmp" / "rollup-docs.pid").read_text() == "4321\n"

    output = capsys.readouterr().out
    assert "Docs available at http://127.0.0.1:8000/" in output
    assert "PID 4321" in output
    assert "Logs: .tmp/rollup-docs.log" in output
    assert "Stop with: kill 4321" in output


def test_docs_command_reuses_existing_live_background_server(monkeypatch, tmp_path, capsys) -> None:
    _create_docs_dir(tmp_path)
    tmp_dir = tmp_path / ".tmp"
    tmp_dir.mkdir()
    (tmp_dir / "rollup-docs.pid").write_text("9876\n")
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
