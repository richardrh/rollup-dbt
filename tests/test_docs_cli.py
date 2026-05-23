from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

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

    exit_code = cli.docs_command(host="0.0.0.0", port=9000, zensical_runner=runner)

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

    exit_code = cli.docs_command(zensical_runner=runner)

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
