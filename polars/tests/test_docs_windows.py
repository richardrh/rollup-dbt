from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def _read_doc(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_step_by_step_guide_uses_windows_compatible_directory_commands():
    guide = _read_doc("docs/load-data.md")

    assert "mkdir -p data/{" not in guide
    assert "New-Item -ItemType Directory -Force" in guide
    assert "data/ylt/verisk" in guide
    assert "data/ep_summaries/risklink" in guide


def test_quickstart_has_windows_install_and_directory_steps():
    quickstart = _read_doc("docs/first-run.md")

    assert "mkdir -p data/ylt/{" not in quickstart
    assert "Copy-Item rollup.example.toml rollup.local.toml" in quickstart
    assert "New-Item -ItemType Directory -Force" in quickstart


def test_docs_index_has_windows_setup_commands():
    index = _read_doc("docs/index.md")

    assert "Windows PowerShell" in index
    assert "Set-Location rollup-dbt" in index
    assert "Copy-Item rollup.example.toml rollup.local.toml" in index
