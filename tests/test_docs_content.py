from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_first_run_combines_ylt_utility_and_ep_summary_guidance() -> None:
    first_run = (REPO_ROOT / "docs" / "first-run.md").read_text(encoding="utf-8")

    assert "DuckDB utility command" in first_run
    assert "utilities.md#convert-a-ylt-csv-extract-to-parquet-with-duckdb" in first_run
    assert "uv run rollup generate-ep-summaries" in first_run
    assert "dist/rollup/rollup generate-ep-summaries" in first_run
    assert "schema.yaml` contracts" in first_run
