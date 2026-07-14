from __future__ import annotations

from pathlib import Path

from rollup import resources


def test_resource_root_source_mode_resolves_repo_root() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    assert resources.resource_root() == repo_root
    assert resources.docs_dir() == repo_root / "docs"
    assert resources.zensical_config_path() == repo_root / "zensical.toml"
