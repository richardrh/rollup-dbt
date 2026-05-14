from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def _read_doc(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_docs_reference_canonical_dbt_style_rollup_layout() -> None:
    docs = "\n".join(
        [
            _read_doc("docs/calculations.md"),
            _read_doc("docs/architecture.md"),
            _read_doc("polars/README.md"),
        ]
    )

    assert "stages.ep_summary" not in docs
    assert "stages.ep.ep_curve_from_ylt" not in docs
    assert "rollup.factors" not in docs
    assert "rollup.staging.ep.ep_curve_from_ylt" in docs
    assert "rollup.intermediate.factors" in docs
    assert "uv run pytest -q" in docs
    assert not re.search(r"\b\d+ passed, \d+ skipped\b", docs)
