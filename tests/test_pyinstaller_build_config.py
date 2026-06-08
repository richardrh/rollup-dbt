from __future__ import annotations

from pathlib import Path
import tomllib


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_build_dependency_group_includes_pyinstaller() -> None:
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert "pyinstaller>=6" in pyproject["dependency-groups"]["build"]


def test_pyinstaller_spec_bundles_docs_config_and_zensical_assets() -> None:
    spec = (REPO_ROOT / "rollup.spec").read_text(encoding="utf-8")

    assert "collect_data_files(\"zensical\"" in spec
    assert "collect_submodules(\"zensical\")" in spec
    assert "collect_submodules(\"markdown\")" in spec
    assert "collect_submodules(\"pymdownx\")" in spec
    assert "collect_submodules(\"pygments.lexers\")" in spec
    assert "ROOT / \"docs\"" in spec
    assert "ROOT / \"zensical.toml\"" in spec
    assert 'name="rollup"' in spec


def test_readme_explains_windows_analyst_bundle_flow() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    assert "send the analyst the whole folder" in readme
    assert "dist/rollup/" in readme
    assert "Do **not** send just `rollup.exe`" in readme
    assert "work/" in readme
    assert "rollup\\rollup.exe validate" in readme
    assert "rollup\\rollup.exe run" in readme
    assert "rollup\\rollup.exe docs" in readme
    assert "work/output/" in readme
