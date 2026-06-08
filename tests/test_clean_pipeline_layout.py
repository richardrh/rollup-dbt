from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_legacy_dbt_model_folders_are_absent() -> None:
    absent_paths = (
        "src/rollup/staging",
        "src/rollup/intermediate",
        "src/rollup/marts",
        "src/rollup/io",
        "src/rollup/pipeline_schema.py",
    )

    assert [path for path in absent_paths if (REPO_ROOT / path).exists()] == []


def test_legacy_rollup_runtime_modules_are_absent() -> None:
    absent_paths = (
        "src/rollup/schemas",
        "src/rollup/seeds.py",
        "src/rollup/config.py",
        "src/rollup/plan.py",
        "src/rollup/plan_render.py",
        "src/rollup/wizard.py",
        "src/rollup/chain.py",
        "src/rollup/audit.py",
        "src/rollup/io",
        "src/rollup/reports",
    )

    assert [path for path in absent_paths if (REPO_ROOT / path).exists()] == []


def test_reduced_test_suite_contains_only_pipeline_tests() -> None:
    expected_tests = {
        "__init__.py",
        "conftest.py",
        "test_cli_docs.py",
        "test_cli_cleanup.py",
        "test_clean_pipeline_layout.py",
        "test_cli_sql.py",
        "test_docs_cli.py",
        "test_docs_content.py",
        "test_api.py",
        "test_ep_summary_generator.py",
        "test_generate_ep_summaries_cli.py",
        "test_pipeline_e2e_validation.py",
        "test_pipeline_fuzz.py",
        "test_pipeline_modelled_dimension_coverage.py",
        "test_pipeline_schema_yaml.py",
        "test_pyinstaller_build_config.py",
        "test_resources.py",
        "test_sql.py",
        "test_sql_integration.py",
    }

    tests_dir = REPO_ROOT / "tests"
    ignored_local_dirs = {"__pycache__", "data", "fuzz"}

    assert {path.name for path in tests_dir.iterdir() if path.name not in ignored_local_dirs} == expected_tests


def test_standard_src_layout_replaces_legacy_polars_folder() -> None:
    assert (REPO_ROOT / "src" / "rollup" / "pipeline.py").is_file()
    assert not (REPO_ROOT / "polars" / "rollup").exists()
    assert not (REPO_ROOT / "polars" / "tests").exists()
    assert not (REPO_ROOT / "polars" / "conftest.py").exists()
    assert not (REPO_ROOT / "polars" / "README.md").exists()

    absent_paths = (
        "polars/RH-TODO-DATA.md",
        "polars/analyst-demo.html",
        "polars/pitch.html",
        "jan-rollup/readme.md",
    )

    assert [path for path in absent_paths if (REPO_ROOT / path).exists()] == []
