from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_pipeline2_dbt_model_folders_are_clean() -> None:
    expected_files = {"__init__.py", "pipeline2.py"}

    for relative in (
        "polars/rollup/staging",
        "polars/rollup/intermediate",
        "polars/rollup/marts",
    ):
        layer = REPO_ROOT / relative
        assert layer.is_dir(), relative
        assert {path.name for path in layer.iterdir() if path.name != "__pycache__"} == expected_files


def test_legacy_rollup_runtime_modules_are_absent() -> None:
    absent_paths = (
        "polars/rollup/pipeline.py",
        "polars/rollup/schemas",
        "polars/rollup/seeds.py",
        "polars/rollup/config.py",
        "polars/rollup/plan.py",
        "polars/rollup/plan_render.py",
        "polars/rollup/wizard.py",
        "polars/rollup/cli.py",
        "polars/rollup/chain.py",
        "polars/rollup/audit.py",
        "polars/rollup/io",
        "polars/rollup/reports",
    )

    assert [path for path in absent_paths if (REPO_ROOT / path).exists()] == []


def test_reduced_test_suite_contains_only_pipeline2_tests() -> None:
    expected_tests = {
        "__init__.py",
        "test_clean_pipeline2_layout.py",
        "test_pipeline2.py",
        "test_pipeline2_schema.py",
        "test_pipeline2_schema_yaml.py",
    }

    tests_dir = REPO_ROOT / "polars" / "tests"

    assert {path.name for path in tests_dir.iterdir() if path.name != "__pycache__"} == expected_tests


def test_legacy_docs_are_absent_and_pipeline2_docs_remain() -> None:
    assert (REPO_ROOT / "polars" / "README.md").is_file()
    assert (REPO_ROOT / "docs" / "index.md").is_file()
    assert {path.name for path in (REPO_ROOT / "docs").iterdir()} == {"index.md"}

    absent_paths = (
        "polars/RH-TODO-DATA.md",
        "polars/analyst-demo.html",
        "polars/pitch.html",
        "jan-rollup/readme.md",
    )

    assert [path for path in absent_paths if (REPO_ROOT / path).exists()] == []
