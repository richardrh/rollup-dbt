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


def test_programmatic_api_docs_include_dataiku_recipes() -> None:
    api_docs = (REPO_ROOT / "docs" / "programmatic-api.md").read_text(
        encoding="utf-8"
    )

    assert "Dataiku recipe: local managed folders" in api_docs
    assert "Dataiku recipe: remote managed folders" in api_docs
    assert "TemporaryDirectory" in api_docs
    assert "get_path()" in api_docs
    assert "get_download_stream" in api_docs
    assert "upload_stream" in api_docs
    assert "run_rollup" in api_docs
