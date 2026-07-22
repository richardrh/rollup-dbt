from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_first_run_combines_ylt_utility_and_ep_summary_guidance() -> None:
    first_run = (REPO_ROOT / "docs" / "first-run.md").read_text(encoding="utf-8")

    assert "DuckDB; see [Utilities]" in first_run
    assert "utilities.md#convert-a-ylt-csv-extract-to-parquet-with-duckdb" in first_run
    assert "uv run rollup generate-ep-summaries" in first_run


def test_programmatic_api_docs_include_dataiku_recipes() -> None:
    api_docs = (REPO_ROOT / "docs" / "programmatic-api.md").read_text(encoding="utf-8")

    assert "Dataiku recipe: local managed folders" in api_docs
    assert "Dataiku recipe: remote managed folders" in api_docs
    assert "TemporaryDirectory" in api_docs
    assert "get_path()" in api_docs
    assert "get_download_stream" in api_docs
    assert "upload_stream" in api_docs
    assert "run_rollup" in api_docs
    assert "log_file" in api_docs
    assert "temporary file" in api_docs


def test_operating_modes_docs_explain_log_file_and_debug_controls() -> None:
    operating_docs = (REPO_ROOT / "docs" / "operating-modes.md").read_text(
        encoding="utf-8"
    )

    assert "--log-file output/run.log" in operating_docs
    assert "--debug" in operating_docs
    assert "--log-level DEBUG" in operating_docs


def test_developer_guide_documents_test_categories_and_quality_commands() -> None:
    developer_guide = (REPO_ROOT / "docs" / "developer-guide.md").read_text(
        encoding="utf-8"
    )

    for command in [
        "uv run pytest -q --run-integration",
        "uv run pytest -q -m integration",
        "uv run pytest -q --run-fuzz",
        "uv run pytest -q -m fuzz",
        "uv run pytest -q --run-integration --run-fuzz",
        "uv run ruff check .",
        "uv run ruff format --check .",
        "uv run mypy src",
        "uv run zensical build --config-file zensical.toml",
    ]:
        assert command in developer_guide

    assert "synthetic fixtures" in developer_guide
    assert "not a\nreal-data smoke pipeline" in developer_guide


def test_docs_do_not_include_sql_statements_or_template_references() -> None:
    docs_text = "\n".join(
        path.read_text(encoding="utf-8") for path in (REPO_ROOT / "docs").glob("*.md")
    )

    prohibited_fragments = [
        "duckdb -c",
        "COPY (",
        "read_csv_auto(",
        "SELECT *",
        "SELECT COUNT",
        "```sql",
        "top-level `sql/`",
        "DuckDB SQL templates",
    ]
    for fragment in prohibited_fragments:
        assert fragment not in docs_text
