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


def test_build_docs_include_pyinstaller_commands() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    building_guide = (REPO_ROOT / "docs" / "building.md").read_text(encoding="utf-8")

    for text in (readme, building_guide):
        assert "uv run --group build pyinstaller -y rollup.spec" in text
        assert "dist/rollup/rollup --help" in text
        assert "dist/rollup/rollup docs" in text
        assert "dist/" in text and "not committed" in text
