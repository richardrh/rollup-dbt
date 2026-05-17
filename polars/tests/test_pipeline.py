from __future__ import annotations

import ast
from pathlib import Path

import polars as pl

from rollup.intermediate.pipeline import apply_vor_adjustments, blend_losses, enrich_losses, select_losses
from rollup.marts.pipeline import build_analysis_loss_summary, build_blended_loss_summary, build_event_loss_fanout, summarize_losses
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
    assert enrich_losses.__module__ == "rollup.intermediate.pipeline"
    assert apply_vor_adjustments.__module__ == "rollup.intermediate.pipeline"
    assert blend_losses.__module__ == "rollup.intermediate.pipeline"
    assert summarize_losses.__module__ == "rollup.marts.pipeline"
    assert build_analysis_loss_summary.__module__ == "rollup.marts.pipeline"
    assert build_event_loss_fanout.__module__ == "rollup.marts.pipeline"
    assert build_blended_loss_summary.__module__ == "rollup.marts.pipeline"
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


def test_intermediate_owns_business_joins_and_vor_adjustments() -> None:
    selected_losses = pl.DataFrame(
        {
            "vendor": ["risklink"],
            "analysis_id": ["200"],
            "year_id": [1847],
            "event_id": [410024195],
            "loss": [100.0],
        }
    ).lazy()
    analyses = pl.DataFrame(
        {"vendor": ["risklink"], "analysis_id": ["200"], "modelled_label": ["EU WS"], "peril_id": [3], "lob_id": [10]}
    ).lazy()
    perils = pl.DataFrame({"peril_id": [3], "name": ["Europe Winter Storm"], "region": ["EU"], "peril_family": ["WS"]}).lazy()
    lobs = pl.DataFrame(
        {
            "lob_id": [10],
            "modelled_lob": ["LOB"],
            "rollup_lob": ["ROLLUP"],
            "lob_type": ["prop"],
            "cds_cat_class_name": ["Class"],
            "office": ["London"],
            "class": ["HH"],
            "currency": ["EUR"],
        }
    ).lazy()
    forecast_factors = pl.DataFrame(
        {"class": ["HH"], "office": ["London"], "office_iso2": ["GB"], "forecast_date": ["2026-01-01"], "factor": [1.5]}
    ).lazy()
    fx_rates = pl.DataFrame(
        {"currency_code": ["EUR"], "target_currency": ["GBP"], "rate_date": ["2026-01-01"], "rate": [0.8]}
    ).lazy()
    euws_rate_factors = pl.DataFrame({"model_event_id": [410024195], "occ_year": [1847], "factor": [2.0]}).lazy()
    blending_weights = pl.DataFrame(
        {
            "peril_id": [3],
            "return_period": [0],
            "peril_name": ["Europe Winter Storm"],
            "description": ["test"],
            "sub_peril": [None],
            "vendor": ["risklink"],
            "base_model": ["verisk"],
            "weight": [0.25],
        },
        schema={
            "peril_id": pl.Int64,
            "return_period": pl.Int64,
            "peril_name": pl.String,
            "description": pl.String,
            "sub_peril": pl.String,
            "vendor": pl.String,
            "base_model": pl.String,
            "weight": pl.Float64,
        },
    ).lazy()

    enriched = enrich_losses(selected_losses, analyses, perils, lobs)
    adjusted = apply_vor_adjustments(enriched, forecast_factors, fx_rates, euws_rate_factors)
    blended = blend_losses(adjusted, blending_weights).collect()

    assert blended.select("peril_name", "rollup_lob", "forecast_factor", "fx_rate", "euws_rate_factor", "adjusted_loss", "blend_weight", "blended_loss").to_dict(as_series=False) == {
        "peril_name": ["Europe Winter Storm"],
        "rollup_lob": ["ROLLUP"],
        "forecast_factor": [1.5],
        "fx_rate": [0.8],
        "euws_rate_factor": [2.0],
        "adjusted_loss": [240.0],
        "blend_weight": [0.25],
        "blended_loss": [60.0],
    }


def test_marts_create_analysis_event_and_blended_fanout_outputs() -> None:
    adjusted_losses = pl.DataFrame(
        {
            "vendor": ["risklink"],
            "analysis_id": ["200"],
            "year_id": [1],
            "event_id": [10],
            "peril_id": [2],
            "peril_name": ["Europe Flood"],
            "region": ["EU"],
            "peril_family": ["FL"],
            "rollup_lob": ["ROLLUP"],
            "currency": ["GBP"],
            "loss": [10.0],
            "forecast_factor": [1.0],
            "fx_rate": [1.0],
            "euws_rate_factor": [1.0],
            "adjusted_loss": [10.0],
        }
    ).lazy()
    blended_losses = adjusted_losses.with_columns(
        pl.lit("risklink").alias("base_model"),
        pl.lit(0.5).alias("blend_weight"),
        pl.lit(5.0).alias("blended_loss"),
    )

    assert build_analysis_loss_summary(adjusted_losses).collect().select("vendor", "analysis_id", "adjusted_total_loss").to_dict(as_series=False) == {
        "vendor": ["risklink"],
        "analysis_id": ["200"],
        "adjusted_total_loss": [10.0],
    }
    assert build_event_loss_fanout(adjusted_losses).collect().select("event_id", "adjusted_loss").to_dict(as_series=False) == {
        "event_id": [10],
        "adjusted_loss": [10.0],
    }
    assert build_blended_loss_summary(blended_losses).collect().select("vendor", "blended_total_loss").to_dict(as_series=False) == {
        "vendor": ["risklink"],
        "blended_total_loss": [5.0],
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
    assert models.reports.artifact is not None
    assert set(models.reports.artifact) == {"loss_summary", "summary_ep_stats"}


def test_pipeline_writes_xlsx_report_when_configured(tmp_path: Path) -> None:
    from rollup.pipeline import build_pipeline

    _write_pipeline_fixture(tmp_path, write_selected=True)
    report_path = tmp_path / "reports" / "rollup.xlsx"

    models = build_pipeline(root=tmp_path, report_path=report_path)

    assert models.reports.xlsx_path == report_path
    assert report_path.is_file()


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
