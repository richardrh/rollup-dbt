from __future__ import annotations

import ast
from pathlib import Path

import polars as pl
import pytest

import rollup.pipeline2 as pipeline2
from rollup.pipeline2 import (
    build_pipeline2,
    build_sources,
    build_staging,
    collect_loss_summary,
    preflight_pipeline2_inputs,
    selected_analyses_spec,
)
from rollup.pipeline2_schema import Pipeline2SchemaError, load_pipeline2_schema


REPO_ROOT = Path(__file__).resolve().parents[2]
LEGACY_RUNTIME_MODULES = {
    "rollup.pipeline",
    "rollup.seeds",
    "rollup.schemas",
    "rollup.staging",
    "rollup.intermediate",
    "rollup.marts",
}
LEGACY_RUNTIME_PATHS = {
    "polars/rollup/pipeline.py",
    "polars/rollup/cli.py",
    "polars/rollup/wizard.py",
    "polars/rollup/config.py",
    "polars/rollup/plan.py",
    "polars/rollup/plan_render.py",
    "polars/rollup/schemas",
    "polars/rollup/seeds.py",
    "polars/rollup/staging",
    "polars/rollup/intermediate",
    "polars/rollup/marts",
    "polars/rollup/reports",
    "polars/rollup/io",
    "polars/rollup/audit.py",
    "polars/rollup/chain.py",
    "polars/rollup/validate.py",
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


def test_staging_queries_are_inline_inside_build_staging() -> None:
    tree = ast.parse((REPO_ROOT / "polars" / "rollup" / "pipeline2.py").read_text(encoding="utf-8"))
    function_names = {
        node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
    }

    assert "stage_risklink_ylt" not in function_names
    assert "stage_verisk_ylt" not in function_names


def test_legacy_runtime_files_are_deleted() -> None:
    assert all(not (REPO_ROOT / path).exists() for path in LEGACY_RUNTIME_PATHS)

    remaining_rollup_files = {
        path.relative_to(REPO_ROOT).as_posix()
        for path in (REPO_ROOT / "polars" / "rollup").glob("*.py")
    }
    assert remaining_rollup_files == {
        "polars/rollup/__init__.py",
        "polars/rollup/pipeline2.py",
        "polars/rollup/pipeline2_schema.py",
    }


def test_pipeline2_tiny_fixture_runs_linear_flow(tmp_path: Path) -> None:
    _write_pipeline2_fixture(tmp_path, selected_name="selected_analyses.csv")

    summary = collect_loss_summary(root=tmp_path)

    assert summary.to_dict(as_series=False) == {
        "vendor": ["risklink", "verisk"],
        "analysis_id": ["200", "100"],
        "total_loss": [10.0, 7.0],
        "event_count": [1, 1],
    }


def test_build_pipeline2_calls_boundary_preflight_before_sources(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _write_pipeline2_fixture(tmp_path, selected_name="selected_analyses.csv")
    order: list[str] = []
    schema = load_pipeline2_schema()

    def fake_preflight(**_kwargs):
        order.append("preflight")
        return schema

    def fake_build_sources(**_kwargs):
        order.append("sources")
        raise RuntimeError("stop after boundary order check")

    monkeypatch.setattr(pipeline2, "preflight_pipeline2_inputs", fake_preflight)
    monkeypatch.setattr(pipeline2, "build_sources", fake_build_sources)

    with pytest.raises(RuntimeError, match="stop after boundary order check"):
        build_pipeline2(root=tmp_path)

    assert order == ["preflight", "sources"]


def test_boundary_preflight_fails_before_transformations(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _write_pipeline2_fixture(tmp_path, selected_name="selected_analyses.csv")
    _write_risklink_ylt(tmp_path / "data" / "ylt" / "risklink" / "risklink.parquet", include_loss=False)
    staging_called = False

    def fail_if_staging_runs(*_args, **_kwargs):
        nonlocal staging_called
        staging_called = True
        raise AssertionError("staging should not run when boundary preflight fails")

    monkeypatch.setattr(pipeline2, "build_staging", fail_if_staging_runs)

    with pytest.raises(Pipeline2SchemaError, match="raw_risklink_ylt.*missing columns.*loss"):
        build_pipeline2(root=tmp_path)

    assert staging_called is False


def test_boundary_preflight_rejects_missing_source_columns(tmp_path: Path) -> None:
    _write_pipeline2_fixture(tmp_path, selected_name="selected_analyses.csv")
    (tmp_path / "data" / "seeds" / "lobs.csv").write_text(
        "lob_id,modelled_lob,rollup_lob,lob_type,cds_cat_class_name,office,class\n"
        "1,modelled,rollup,type,class name,London,CLS\n",
        encoding="utf-8",
    )

    with pytest.raises(Pipeline2SchemaError, match="lobs.*missing columns.*currency"):
        preflight_pipeline2_inputs(root=tmp_path)


def test_boundary_preflight_rejects_extra_source_columns(tmp_path: Path) -> None:
    _write_pipeline2_fixture(tmp_path, selected_name="selected_analyses.csv")
    (tmp_path / "data" / "seeds" / "lobs.csv").write_text(
        "lob_id,modelled_lob,rollup_lob,lob_type,cds_cat_class_name,office,class,currency,debug\n"
        "1,modelled,rollup,type,class name,London,CLS,GBP,true\n",
        encoding="utf-8",
    )

    with pytest.raises(Pipeline2SchemaError, match="lobs.*unexpected columns.*debug"):
        preflight_pipeline2_inputs(root=tmp_path)


def test_boundary_preflight_rejects_wrong_parquet_source_dtype(tmp_path: Path) -> None:
    _write_pipeline2_fixture(tmp_path, selected_name="selected_analyses.csv")
    _write_verisk_ylt(
        tmp_path / "data" / "ylt" / "verisk" / "verisk.parquet",
        event_id_dtype=pl.String,
    )

    with pytest.raises(Pipeline2SchemaError, match="raw_verisk_ylt.*EventID.*expected Int64, got String"):
        preflight_pipeline2_inputs(root=tmp_path)


def test_build_staging_output_matches_yaml_contract(tmp_path: Path) -> None:
    _write_pipeline2_fixture(tmp_path, selected_name="selected_analyses.csv")
    schema = preflight_pipeline2_inputs(root=tmp_path)
    sources = build_sources(root=tmp_path, schema=schema)

    staging = build_staging(sources, schema)

    assert staging.normalized_ylt.collect_schema() == schema.dataset("stg_normalized_ylt").pl_schema


def test_pipeline2_uses_required_selected_analyses_source(tmp_path: Path) -> None:
    _write_pipeline2_fixture(tmp_path, selected_name="selected_analyses.csv")

    spec = selected_analyses_spec(load_pipeline2_schema(), root=tmp_path)

    assert spec.name == "selected_analyses"
    assert spec.status == "first_class"


def test_pipeline2_requires_selected_analyses_at_boundary(tmp_path: Path) -> None:
    _write_pipeline2_fixture(tmp_path, selected_name="selected_analyses.csv")
    (tmp_path / "data" / "seeds" / "selected_analyses.csv").unlink()

    with pytest.raises(Pipeline2SchemaError, match="selected_analyses is required"):
        preflight_pipeline2_inputs(root=tmp_path)


def _write_pipeline2_fixture(tmp_path: Path, *, selected_name: str) -> None:
    seeds = tmp_path / "data" / "seeds"
    risklink = tmp_path / "data" / "ylt" / "risklink"
    verisk = tmp_path / "data" / "ylt" / "verisk"
    seeds.mkdir(parents=True)
    risklink.mkdir(parents=True)
    verisk.mkdir(parents=True)

    _write_required_seed_sources(seeds)

    (seeds / selected_name).write_text(
        "vendor,analysis_id\nrisklink,200\nverisk,100\n",
        encoding="utf-8",
    )
    _write_risklink_ylt(risklink / "risklink.parquet")
    _write_verisk_ylt(verisk / "verisk.parquet")


def _write_required_seed_sources(seeds: Path) -> None:
    (seeds / "lobs.csv").write_text(
        "lob_id,modelled_lob,rollup_lob,lob_type,cds_cat_class_name,office,class,currency\n"
        "1,modelled,rollup,type,class name,London,CLS,GBP\n",
        encoding="utf-8",
    )
    (seeds / "perils.csv").write_text(
        "peril_id,name,region,peril_family\n1,Wind,EU,WS\n",
        encoding="utf-8",
    )
    (seeds / "analyses.csv").write_text(
        "vendor,analysis_id,modelled_label,peril_id,lob_id\n"
        "risklink,200,RL analysis,1,1\n"
        "verisk,100,VK analysis,1,1\n",
        encoding="utf-8",
    )
    (seeds / "blending_weights.csv").write_text(
        "peril_id,return_period,peril_name,description,sub_peril,vendor,base_model,weight\n"
        "1,100,Wind,Wind peril,,risklink,risklink,1.0\n",
        encoding="utf-8",
    )
    (seeds / "forecast_factors.csv").write_text(
        "class,office,office_iso2,forecast_date,factor\nCLS,London,GB,2026-01-01,1.0\n",
        encoding="utf-8",
    )
    (seeds / "fx_rates.csv").write_text(
        "currency_code,target_currency,rate_date,rate\nGBP,GBP,2026-01-01,1.0\n",
        encoding="utf-8",
    )
    (seeds / "euws_rate_factors.csv").write_text(
        "model_event_id,occ_year,factor\n1,2026,1.0\n",
        encoding="utf-8",
    )


def _write_risklink_ylt(path: Path, *, include_loss: bool = True) -> None:
    data = {
        "yearid": [1, 1],
        "eventid": [10, 11],
        "p_value": [0.1, 0.2],
        "anlsid": [200, 999],
        "meanloss": [10.0, 99.0],
        "stddev": [0.0, 0.0],
        "expvalue": [10.0, 99.0],
    }
    schema = {
        "yearid": pl.Int64,
        "eventid": pl.Int64,
        "p_value": pl.Float64,
        "anlsid": pl.Int64,
        "meanloss": pl.Float64,
        "stddev": pl.Float64,
        "expvalue": pl.Float64,
    }
    if include_loss:
        data["loss"] = [10.0, 99.0]
        schema["loss"] = pl.Float64

    pl.DataFrame(data, schema=schema).write_parquet(path)


def _write_verisk_ylt(path: Path, *, event_id_dtype: pl.DataType = pl.Int64) -> None:
    event_ids = ["20", "21"] if event_id_dtype == pl.String else [20, 21]
    pl.DataFrame(
        {
            "Analysis": ["100", "998"],
            "ExposureAttribute": ["lob", "lob"],
            "CatalogTypeCode": ["STC", "STC"],
            "EventID": event_ids,
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
            "EventID": event_id_dtype,
            "ModelCode": pl.Int64,
            "YearID": pl.Int64,
            "PerilSetCode": pl.Int64,
            "GroundUpLoss": pl.Float64,
            "GrossLoss": pl.Float64,
            "NetOfPreCatLoss": pl.Float64,
            "filename": pl.String,
        },
    ).write_parquet(path)
