from __future__ import annotations

import ast
from pathlib import Path

import polars as pl

from rollup.intermediate.pipeline import select_losses
from rollup.marts.pipeline import summarize_losses
from rollup.reports.pipeline import prepare_summary_ep_stats
from rollup.pipeline import collect_loss_summary, load_dataset, preflight_pipeline_boundary, selected_analyses_spec
from rollup.pipeline_schema import PipelineSchemaError, load_pipeline_schema
from rollup.staging.pipeline import build_normalized_ylt, stage_risklink_ylt, stage_selected_analyses, stage_verisk_ylt


REPO_ROOT = Path(__file__).resolve().parents[2]
LEGACY_RUNTIME_MODULES = {
    "rollup.pipeline",
    "rollup.seeds",
    "rollup.schemas",
}


def test_pipeline_does_not_import_legacy_runtime_modules() -> None:
    for relative in ("polars/rollup/pipeline.py", "polars/rollup/pipeline_schema.py"):
        tree = ast.parse((REPO_ROOT / relative).read_text(encoding="utf-8"))
        imported_modules: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imported_modules.append(node.module)

        assert not (set(imported_modules) & LEGACY_RUNTIME_MODULES)


def test_pipeline_model_query_functions_are_in_dbt_layer_modules() -> None:
    assert stage_selected_analyses.__module__ == "rollup.staging.pipeline"
    assert stage_risklink_ylt.__module__ == "rollup.staging.pipeline"
    assert stage_verisk_ylt.__module__ == "rollup.staging.pipeline"
    assert build_normalized_ylt.__module__ == "rollup.staging.pipeline"
    assert select_losses.__module__ == "rollup.intermediate.pipeline"
    assert summarize_losses.__module__ == "rollup.marts.pipeline"
    assert prepare_summary_ep_stats.__module__ == "rollup.reports.pipeline"


def test_pipeline_orchestrator_keeps_preflight_before_transformations() -> None:
    tree = ast.parse((REPO_ROOT / "polars/rollup/pipeline.py").read_text(encoding="utf-8"))
    build_pipeline = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "build_pipeline"
    )
    call_order = [
        call.func.id
        for call in ast.walk(build_pipeline)
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
    ]

    assert call_order.index("preflight_pipeline_boundary") < call_order.index("build_staging")
    assert call_order.index("build_staging") < call_order.index("build_intermediate")
    assert call_order.index("build_intermediate") < call_order.index("build_marts")
    assert call_order.index("build_marts") < call_order.index("build_reports")


def test_pipeline_preflight_failure_stops_before_stage_execution(monkeypatch) -> None:
    from rollup import pipeline as pipeline_module
    from rollup.pipeline import PipelineSources
    from rollup.staging.pipeline import load_configured_sources

    schema = load_pipeline_schema()
    datasets = dict(load_configured_sources(root=REPO_ROOT, schema=schema).datasets)
    datasets["selected_analyses"] = pl.DataFrame({"vendor": ["risklink"]}).lazy()
    invalid_sources = PipelineSources(
        selected_analyses=datasets["selected_analyses"],
        raw_ylt=(datasets["raw_risklink_ylt"], datasets["raw_verisk_ylt"]),
        datasets=datasets,
    )

    monkeypatch.setattr(pipeline_module, "build_sources", lambda *, root, schema: invalid_sources)

    def fail_build_staging(*args, **kwargs):
        raise AssertionError("staging should not run after preflight failure")

    monkeypatch.setattr(pipeline_module, "build_staging", fail_build_staging)

    try:
        pipeline_module.build_pipeline(root=REPO_ROOT, schema=schema)
    except PipelineSchemaError as exc:
        assert "missing columns" in str(exc)
    else:
        raise AssertionError("preflight should reject invalid sources")


def test_pipeline_tiny_fixture_runs_linear_flow(tmp_path: Path) -> None:
    _write_pipeline_fixture(tmp_path, write_selected=True)

    summary = collect_loss_summary(root=tmp_path)

    assert summary.to_dict(as_series=False) == {
        "vendor": ["risklink", "verisk"],
        "analysis_id": ["200", "100"],
        "total_loss": [10.0, 7.0],
        "event_count": [1, 1],
    }


def test_pipeline_report_hooks_produce_summary_ep_stats(tmp_path: Path) -> None:
    from rollup.pipeline import build_pipeline

    _write_pipeline_fixture(tmp_path, write_selected=True)

    models = build_pipeline(root=tmp_path)

    assert isinstance(models.reports.summary_ep_stats, pl.LazyFrame)
    assert models.reports.summary_ep_stats.collect().to_dict(as_series=False) == {
        "analysis_count": [2],
        "portfolio_total_loss": [17.0],
        "portfolio_event_count": [2],
    }


def test_pipeline_requires_selected_analyses(tmp_path: Path) -> None:
    _write_pipeline_fixture(tmp_path, write_selected=False)

    try:
        selected_analyses_spec(load_pipeline_schema(), root=tmp_path)
    except PipelineSchemaError as exc:
        assert str(exc) == "selected_analyses is required"
    else:
        raise AssertionError("missing selected analyses should fail")


def test_pipeline_selected_analyses_uses_business_seed_path(tmp_path: Path) -> None:
    _write_pipeline_fixture(tmp_path, write_selected=True)

    spec = selected_analyses_spec(load_pipeline_schema(), root=tmp_path)

    assert spec.name == "selected_analyses"
    assert spec.path == "data/seeds/business/selected_analyses.csv"


def test_selected_analyses_template_validates_at_boundary() -> None:
    from rollup.pipeline import build_sources

    schema = load_pipeline_schema()

    preflight_pipeline_boundary(
        build_sources(root=REPO_ROOT, schema=schema),
        schema,
    )


def _write_pipeline_fixture(tmp_path: Path, *, write_selected: bool) -> None:
    seeds = tmp_path / "data" / "seeds" / "business"
    vor = tmp_path / "data" / "seeds" / "vor"
    risklink = tmp_path / "data" / "ylt" / "risklink"
    verisk = tmp_path / "data" / "ylt" / "verisk"
    seeds.mkdir(parents=True)
    vor.mkdir(parents=True)
    risklink.mkdir(parents=True)
    verisk.mkdir(parents=True)

    if write_selected:
        (seeds / "selected_analyses.csv").write_text(
            "vendor,analysis_id\nrisklink,200\nverisk,100\n",
            encoding="utf-8",
        )
    (seeds / "lobs.csv").write_text(
        "lob_id,modelled_lob,rollup_lob,lob_type,cds_cat_class_name,office,class,currency\n",
        encoding="utf-8",
    )
    (seeds / "perils.csv").write_text("peril_id,name,region,peril_family\n", encoding="utf-8")
    (seeds / "analyses.csv").write_text(
        "vendor,analysis_id,modelled_label,peril_id,lob_id\n",
        encoding="utf-8",
    )
    (vor / "blending_weights.csv").write_text(
        "peril_id,return_period,peril_name,description,sub_peril,vendor,base_model,weight\n",
        encoding="utf-8",
    )
    (vor / "forecast_factors.csv").write_text(
        "class,office,office_iso2,forecast_date,factor\n",
        encoding="utf-8",
    )
    (vor / "fx_rates.csv").write_text("currency_code,target_currency,rate_date,rate\n", encoding="utf-8")
    (vor / "euws_rate_factors.csv").write_text("model_event_id,occ_year,factor\n", encoding="utf-8")
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


def _sources_with_selected_analyses(selected_analyses: pl.LazyFrame):
    from rollup.pipeline import PipelineSources

    raw_risklink_ylt = pl.DataFrame(
        {
            "yearid": [1],
            "eventid": [10],
            "p_value": [0.1],
            "anlsid": [200],
            "meanloss": [10.0],
            "stddev": [0.0],
            "expvalue": [10.0],
            "loss": [10.0],
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
    ).lazy()
    raw_verisk_ylt = pl.DataFrame(
        {
            "Analysis": ["100"],
            "ExposureAttribute": ["lob"],
            "CatalogTypeCode": ["STC"],
            "EventID": [20],
            "ModelCode": [1],
            "YearID": [1],
            "PerilSetCode": [10],
            "GroundUpLoss": [7.0],
            "GrossLoss": [7.0],
            "NetOfPreCatLoss": [7.0],
            "filename": ["vk.parquet"],
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
    ).lazy()
    return PipelineSources(selected_analyses=selected_analyses, raw_ylt=(raw_risklink_ylt, raw_verisk_ylt))
