from __future__ import annotations

import ast
from pathlib import Path

import polars as pl

from rollup.intermediate.pipeline2 import select_losses
from rollup.marts.pipeline2 import summarize_losses
from rollup.pipeline2 import collect_loss_summary, preflight_pipeline2_boundary, selected_analyses_spec
from rollup.pipeline2_schema import Pipeline2SchemaError, load_pipeline2_schema
from rollup.staging.pipeline2 import build_normalized_ylt, stage_risklink_ylt, stage_selected_analyses, stage_verisk_ylt


REPO_ROOT = Path(__file__).resolve().parents[2]
LEGACY_RUNTIME_MODULES = {
    "rollup.pipeline",
    "rollup.seeds",
    "rollup.schemas",
}


def test_pipeline2_does_not_import_legacy_runtime_modules() -> None:
    for relative in ("polars/rollup/pipeline2.py", "polars/rollup/pipeline2_schema.py"):
        tree = ast.parse((REPO_ROOT / relative).read_text(encoding="utf-8"))
        imported_modules: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imported_modules.append(node.module)

        assert not (set(imported_modules) & LEGACY_RUNTIME_MODULES)


def test_pipeline2_model_query_functions_are_in_dbt_layer_modules() -> None:
    assert stage_selected_analyses.__module__ == "rollup.staging.pipeline2"
    assert stage_risklink_ylt.__module__ == "rollup.staging.pipeline2"
    assert stage_verisk_ylt.__module__ == "rollup.staging.pipeline2"
    assert build_normalized_ylt.__module__ == "rollup.staging.pipeline2"
    assert select_losses.__module__ == "rollup.intermediate.pipeline2"
    assert summarize_losses.__module__ == "rollup.marts.pipeline2"


def test_pipeline2_orchestrator_keeps_preflight_before_transformations() -> None:
    tree = ast.parse((REPO_ROOT / "polars/rollup/pipeline2.py").read_text(encoding="utf-8"))
    build_pipeline2 = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "build_pipeline2"
    )
    call_order = [
        call.func.id
        for call in ast.walk(build_pipeline2)
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
    ]

    assert call_order.index("preflight_pipeline2_boundary") < call_order.index("build_staging")


def test_pipeline2_tiny_fixture_runs_linear_flow(tmp_path: Path) -> None:
    _write_pipeline2_fixture(tmp_path, write_selected=True)

    summary = collect_loss_summary(root=tmp_path)

    assert summary.to_dict(as_series=False) == {
        "vendor": ["risklink", "verisk"],
        "analysis_id": ["200", "100"],
        "total_loss": [10.0, 7.0],
        "event_count": [1, 1],
    }


def test_pipeline2_requires_selected_analyses(tmp_path: Path) -> None:
    _write_pipeline2_fixture(tmp_path, write_selected=False)

    try:
        selected_analyses_spec(load_pipeline2_schema(), root=tmp_path)
    except Pipeline2SchemaError as exc:
        assert str(exc) == "selected_analyses is required"
    else:
        raise AssertionError("missing selected analyses should fail")


def test_pipeline2_selected_analyses_uses_business_seed_path(tmp_path: Path) -> None:
    _write_pipeline2_fixture(tmp_path, write_selected=True)

    spec = selected_analyses_spec(load_pipeline2_schema(), root=tmp_path)

    assert spec.name == "selected_analyses"
    assert spec.path == "data/seeds/business/selected_analyses.csv"


def _write_pipeline2_fixture(tmp_path: Path, *, write_selected: bool) -> None:
    seeds = tmp_path / "data" / "seeds" / "business"
    risklink = tmp_path / "data" / "ylt" / "risklink"
    verisk = tmp_path / "data" / "ylt" / "verisk"
    seeds.mkdir(parents=True)
    risklink.mkdir(parents=True)
    verisk.mkdir(parents=True)

    if write_selected:
        (seeds / "selected_analyses.csv").write_text(
            "vendor,analysis_id\nrisklink,200\nverisk,100\n",
            encoding="utf-8",
        )
    pl.DataFrame(
        {
            "yearid": [1, 1],
            "eventid": [10, 11],
            "p_value": [0.1, 0.2],
            "anlsid": [200, 999],
            "meanloss": [10.0, 99.0],
            "stddev": [0.0, 0.0],
            "expvalue": [10.0, 99.0],
            "loss": [10.0, 99.0],
        },
        schema={
            "yearid": pl.Int64,
            "eventid": pl.Int64,
            "p_value": pl.Float64,
            "anlsid": pl.Int64,
            "meanloss": pl.Float64,
            "stddev": pl.Float64,
            "expvalue": pl.Float64,
            "loss": pl.Float64,
        },
    ).write_parquet(risklink / "risklink.parquet")
    pl.DataFrame(
        {
            "Analysis": ["100", "998"],
            "ExposureAttribute": ["lob", "lob"],
            "CatalogTypeCode": ["STC", "STC"],
            "EventID": [20, 21],
            "ModelCode": [1, 1],
            "YearID": [1, 1],
            "PerilSetCode": [10, 10],
            "GroundUpLoss": [7.0, 88.0],
            "GrossLoss": [7.0, 88.0],
            "NetOfPreCatLoss": [7.0, 88.0],
            "filename": ["vk.parquet", "vk.parquet"],
        },
        schema={
            "Analysis": pl.String,
            "ExposureAttribute": pl.String,
            "CatalogTypeCode": pl.String,
            "EventID": pl.Int64,
            "ModelCode": pl.Int64,
            "YearID": pl.Int64,
            "PerilSetCode": pl.Int64,
            "GroundUpLoss": pl.Float64,
            "GrossLoss": pl.Float64,
            "NetOfPreCatLoss": pl.Float64,
            "filename": pl.String,
        },
    ).write_parquet(verisk / "verisk.parquet")
