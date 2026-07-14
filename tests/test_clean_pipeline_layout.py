from __future__ import annotations

import ast
from pathlib import Path
import tomllib


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_dbt_like_polars_model_layout_is_present() -> None:
    expected_paths = (
        "src/rollup/sources/catalog.py",
        "src/rollup/sources/ylt.py",
        "src/rollup/staging/stg_ep_summaries.py",
        "src/rollup/staging/stg_factors.py",
        "src/rollup/staging/stg_ylt.py",
        "src/rollup/staging/stg_event_catalogues.py",
        "src/rollup/intermediate/int_ep.py",
        "src/rollup/intermediate/int_ylt_main.py",
        "src/rollup/intermediate/int_ylt_dialsup.py",
        "src/rollup/marts/mart_fanout.py",
        "src/rollup/marts/mart_wide.py",
        "src/rollup/writers/parquet.py",
        "src/rollup/writers/debug.py",
        "src/rollup/writers/duckdb_export.py",
        "src/rollup/writers/fanout_partitions.py",
        "src/rollup/writers/wide_outputs.py",
        "src/rollup/pipeline.py",
    )

    assert [path for path in expected_paths if not (REPO_ROOT / path).is_file()] == []


def test_wheel_package_discovery_includes_rollup_subpackages() -> None:
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert pyproject["tool"]["setuptools"]["packages"]["find"] == {
        "where": ["src"],
        "include": ["rollup", "rollup.*"],
    }


def test_dbt_like_modules_do_not_import_pipeline_facade() -> None:
    model_roots = (
        REPO_ROOT / "src" / "rollup" / "staging",
        REPO_ROOT / "src" / "rollup" / "intermediate",
        REPO_ROOT / "src" / "rollup" / "marts",
        REPO_ROOT / "src" / "rollup" / "writers",
    )

    offenders = []
    for root in model_roots:
        for path in root.rglob("*.py"):
            text = path.read_text()
            if "from rollup.pipeline import" in text or "import rollup.pipeline" in text:
                offenders.append(path.relative_to(REPO_ROOT).as_posix())

    assert offenders == []


def test_ylt_intermediate_models_are_not_placeholder_builder_aliases() -> None:
    files = (
        REPO_ROOT / "src" / "rollup" / "intermediate" / "int_ylt_main.py",
        REPO_ROOT / "src" / "rollup" / "intermediate" / "int_ylt_dialsup.py",
    )

    offenders = [
        path.relative_to(REPO_ROOT).as_posix()
        for path in files
        if " = build_main_ylt_metrics" in path.read_text()
        or " = build_dialsup_ylt_metrics" in path.read_text()
    ]

    assert offenders == []


def test_pipeline_orchestration_imports_public_model_apis() -> None:
    pipeline = ast.parse((REPO_ROOT / "src" / "rollup" / "pipeline.py").read_text())
    model_prefixes = (
        "rollup.sources.",
        "rollup.staging.",
        "rollup.intermediate.",
        "rollup.marts.",
    )
    offenders: list[str] = []
    for node in ast.walk(pipeline):
        if not isinstance(node, ast.ImportFrom) or node.module is None:
            continue
        if not node.module.startswith(model_prefixes):
            continue
        for alias in node.names:
            if alias.name.startswith("_"):
                offenders.append(f"{node.module}.{alias.name}")

    assert offenders == []


def test_removed_pipeline_wrappers_and_obsolete_sql_helper_are_absent() -> None:
    source_files = [path for path in (REPO_ROOT / "src" / "rollup").rglob("*.py")]
    text_by_file = {path.relative_to(REPO_ROOT).as_posix(): path.read_text() for path in source_files}

    assert all("PipelineStage" not in text for text in text_by_file.values())
    assert all("EpBlendingTargets" not in text for text in text_by_file.values())
    assert all("def _sql_frame" not in text for text in text_by_file.values())
    assert all("def int_ylt_main_ranked_blended" not in text for text in text_by_file.values())


def test_sql_server_and_pyinstaller_runtime_support_are_absent() -> None:
    assert not (REPO_ROOT / "src" / "rollup" / "sql.py").exists()
    assert not (REPO_ROOT / "rollup.spec").exists()
    assert not (REPO_ROOT / "scripts" / "build.py").exists()

    source_files = [path for path in (REPO_ROOT / "src" / "rollup").rglob("*.py")]
    text_by_file = {path.relative_to(REPO_ROOT).as_posix(): path.read_text() for path in source_files}
    forbidden_tokens = (
        "pyodbc",
        "sqlalchemy",
        "rollup.sql",
        "sql-check",
        "test-sql",
        "check_sql_connection",
        "push_mart_parquets_to_sql",
        "PyInstaller",
        "pyinstaller",
        "_MEIPASS",
        "is_frozen",
        "sys.frozen",
    )

    offenders = {
        filename: [token for token in forbidden_tokens if token in text]
        for filename, text in text_by_file.items()
        if any(token in text for token in forbidden_tokens)
    }
    assert offenders == {}


def test_marts_are_pure_transformations_without_io_or_duckdb_processes() -> None:
    offenders: list[str] = []
    forbidden_tokens = (
        "import duckdb",
        "import subprocess",
        "subprocess.",
        "sink_parquet",
        "write_parquet",
        "write_parquet_with_log",
        "mkdir(",
        "unlink(",
        "TemporaryDirectory",
    )
    for path in (REPO_ROOT / "src" / "rollup" / "marts").rglob("*.py"):
        text = path.read_text()
        if any(token in text for token in forbidden_tokens):
            offenders.append(path.relative_to(REPO_ROOT).as_posix())

    assert offenders == []


def test_seed_staging_pass_through_module_is_absent() -> None:
    assert not (REPO_ROOT / "src" / "rollup" / "staging" / "stg_seeds.py").exists()


def test_old_seed_catalog_stack_is_absent() -> None:
    source_text = (REPO_ROOT / "src" / "rollup" / "sources" / "catalog.py").read_text()
    pipeline_types_text = (REPO_ROOT / "src" / "rollup" / "pipeline_types.py").read_text()

    for legacy_name in (
        "SourceEntry",
        "SourceCatalog",
        "SeedCatalog",
        "discover_seed_catalog",
        "load_validated_seed_frames",
    ):
        assert legacy_name not in source_text
    assert "SeedValidationResult" not in pipeline_types_text
    assert "EpSummaryValidationResult" not in pipeline_types_text
    assert not (REPO_ROOT / "src" / "rollup" / "seed_lookup.py").exists()
    assert not (REPO_ROOT / "src" / "rollup" / "sources" / "ep_summaries.py").exists()


def test_ep_staging_and_intermediate_have_no_removed_wrapper_compatibility() -> None:
    stg_ep_text = (REPO_ROOT / "src" / "rollup" / "staging" / "stg_ep_summaries.py").read_text()
    int_ep_text = (REPO_ROOT / "src" / "rollup" / "intermediate" / "int_ep.py").read_text()
    pipeline_types_text = (REPO_ROOT / "src" / "rollup" / "pipeline_types.py").read_text()

    assert "def stage_ep_summaries" not in stg_ep_text
    assert "isinstance(enriched, tuple)" not in int_ep_text
    assert "hasattr(enriched, \"selected\")" not in int_ep_text
    assert "JoinedEpSummaries" not in pipeline_types_text


def test_model_modules_are_not_import_only_alias_wrappers() -> None:
    model_roots = (
        REPO_ROOT / "src" / "rollup" / "staging",
        REPO_ROOT / "src" / "rollup" / "intermediate",
        REPO_ROOT / "src" / "rollup" / "marts",
    )
    offenders: list[str] = []
    for root in model_roots:
        for path in root.rglob("*.py"):
            if path.name == "__init__.py":
                continue
            module = ast.parse(path.read_text())
            meaningful = [
                node
                for node in module.body
                if not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str))
            ]
            if meaningful and all(isinstance(node, (ast.Import, ast.ImportFrom, ast.Assign, ast.AnnAssign)) for node in meaningful):
                offenders.append(path.relative_to(REPO_ROOT).as_posix())

    assert offenders == []


def test_legacy_rollup_runtime_modules_are_absent() -> None:
    absent_paths = (
        "src/rollup/schemas",
        "src/rollup/seeds.py",
        "src/rollup/plan.py",
        "src/rollup/plan_render.py",
        "src/rollup/wizard.py",
        "src/rollup/chain.py",
        "src/rollup/audit.py",
        "src/rollup/reports",
    )

    assert [path for path in absent_paths if (REPO_ROOT / path).exists()] == []


def test_reduced_test_suite_contains_only_pipeline_tests() -> None:
    expected_tests = {
        "__init__.py",
        "conftest.py",
        "test_cli_docs.py",
        "test_cli_entrypoint.py",
        "test_cli_cleanup.py",
        "test_clean_pipeline_layout.py",
        "test_config.py",
        "test_docs_cli.py",
        "test_docs_content.py",
        "test_api.py",
        "test_ep_summary_generator.py",
        "test_generate_ep_summaries_cli.py",
        "test_pipeline_e2e_validation.py",
        "test_pipeline_fuzz.py",
        "test_pipeline_modelled_dimension_coverage.py",
        "test_perils_seed.py",
        "test_duckdb_export.py",
        "test_logging.py",
        "test_risklink_fanout_join.py",
        "test_resources.py",
        "test_validation_catalogue_inputs.py",
        "test_source_catalog.py",
        "test_ep_summary_sources.py",
        "test_ylt_source_staging.py",
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
