from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

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


def _use_tmp_docs_project(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(rollup_resources, "resource_root", lambda: tmp_path)
    monkeypatch.setattr(rollup_resources, "is_frozen", lambda: False)


def test_docs_command_uses_zensical_runner(monkeypatch, tmp_path, capsys) -> None:
    _write_docs_project(tmp_path)
    _use_tmp_docs_project(monkeypatch, tmp_path)
    runner_calls: list[list[str]] = []

    def runner(args: Sequence[str]) -> int:
        runner_calls.append(list(args))
        return 7

    assert cli.docs_command(
        host="0.0.0.0",
        port=9000,
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
