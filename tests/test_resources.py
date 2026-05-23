from __future__ import annotations

from pathlib import Path
import sys

from rollup import resources


def test_resource_root_source_mode_resolves_repo_root() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    assert resources.resource_root() == repo_root
    assert resources.docs_dir() == repo_root / "docs"
    assert resources.zensical_config_path() == repo_root / "zensical.toml"


def test_resource_root_frozen_mode_uses_pyinstaller_meipass(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bundle_root = tmp_path / "_internal"
    bundle_root.mkdir()
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "_MEIPASS", str(bundle_root), raising=False)

    assert resources.resource_root() == bundle_root
    assert resources.resource_path("docs") == bundle_root / "docs"


def test_resource_root_frozen_mode_falls_back_to_executable_parent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    executable = tmp_path / "rollup"
    executable.write_text("", encoding="utf-8")
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.delattr(sys, "_MEIPASS", raising=False)
    monkeypatch.setattr(sys, "executable", str(executable))

    assert resources.resource_root() == tmp_path
