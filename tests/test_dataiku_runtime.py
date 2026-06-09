from __future__ import annotations

import ast
import importlib
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from rollup.api import run_rollup
from rollup.config import load_config
from rollup.schemas import SchemaGuardError, require_columns


def test_import_surface() -> None:
    import rollup
    from rollup.api import run_rollup as api_run_rollup
    from rollup.pipeline import run
    from rollup.staging import load_sources, normalize_ylt, stage_ep_summaries

    assert rollup.run_rollup is api_run_rollup
    assert callable(run)
    assert callable(load_sources)
    assert callable(normalize_ylt)
    assert callable(stage_ep_summaries)


def test_transform_modules_are_split_into_packages() -> None:
    expected_members = {
        "rollup.staging.load_sources": "load_sources",
        "rollup.staging.normalize_ylt": "normalize_ylt",
        "rollup.staging.stage_ep_summaries": "stage_ep_summaries",
        "rollup.intermediate.build_enriched_ylt": "build_enriched_ylt",
        "rollup.intermediate.apply_blending": "apply_blending",
        "rollup.intermediate.apply_fx": "apply_fx",
        "rollup.intermediate.apply_forecast": "apply_forecast",
        "rollup.intermediate.apply_euws": "apply_euws",
        "rollup.intermediate.build_metric_long": "build_metric_long",
        "rollup.intermediate.build_dialsup": "build_dialsup",
        "rollup.marts.write_stage_frames": "write_stage_frames",
        "rollup.marts.write_marts": "write_marts",
        "rollup.marts.write_parquet": "write_parquet",
        "rollup.marts.wide": "wide",
        "rollup.marts.event_validation": "event_validation",
        "rollup.marts.fanouts": "write_fanouts",
    }

    for module_name, member_name in expected_members.items():
        module = importlib.import_module(module_name)

        assert callable(getattr(module, member_name))


def test_transform_modules_expose_polars_schema_contracts() -> None:
    expected_schemas = {
        "rollup.staging.load_sources": (
            "VERISK_YLT_SCHEMA",
            "RISKLINK_YLT_SCHEMA",
            "EP_SUMMARY_SCHEMA",
            "LOBS_SCHEMA",
            "PERILS_SCHEMA",
        ),
        "rollup.staging.normalize_ylt": (
            "NORMALIZE_VERISK_INPUT_SCHEMA",
            "NORMALIZE_RISKLINK_INPUT_SCHEMA",
            "NORMALIZED_YLT_SCHEMA",
            "NORMALIZE_YLT_OUTPUT_SCHEMA",
        ),
        "rollup.staging.stage_ep_summaries": (
            "STAGED_EP_SUMMARIES_INPUT_SCHEMA",
            "STAGED_EP_SUMMARIES_LOBS_INPUT_SCHEMA",
            "STAGED_EP_SUMMARIES_PERILS_INPUT_SCHEMA",
            "STAGED_EP_SUMMARIES_OUTPUT_SCHEMA",
        ),
        "rollup.intermediate.build_enriched_ylt": (
            "ENRICHED_YLT_INPUT_SCHEMA",
            "ENRICHED_EP_INPUT_SCHEMA",
            "ENRICHED_YLT_OUTPUT_SCHEMA",
        ),
        "rollup.intermediate.apply_blending": (
            "BLENDING_INPUT_SCHEMA",
            "BLENDING_FACTORS_SCHEMA",
            "BLENDED_YLT_SCHEMA",
        ),
        "rollup.intermediate.apply_fx": (
            "FX_INPUT_SCHEMA",
            "FX_RATES_SCHEMA",
            "FX_APPLIED_YLT_SCHEMA",
        ),
        "rollup.intermediate.apply_forecast": (
            "FORECAST_INPUT_SCHEMA",
            "FORECAST_FACTORS_SCHEMA",
            "FORECAST_APPLIED_YLT_SCHEMA",
        ),
        "rollup.intermediate.apply_euws": (
            "EUWS_INPUT_SCHEMA",
            "EUWS_FACTORS_SCHEMA",
            "EUWS_APPLIED_YLT_SCHEMA",
        ),
        "rollup.intermediate.build_metric_long": (
            "METRIC_LONG_INPUT_SCHEMA",
            "METRIC_LONG_SCHEMA",
        ),
        "rollup.intermediate.build_dialsup": (
            "DIALSUP_INPUT_SCHEMA",
            "DIALSUP_SCHEMA",
        ),
        "rollup.marts.event_validation": (
            "EVENT_VALIDATION_INPUT_SCHEMA",
            "EVENT_VALIDATION_SCHEMA",
        ),
        "rollup.marts.fanouts": ("FANOUT_INPUT_SCHEMA", "FANOUT_SCHEMA"),
        "rollup.marts.wide": ("WIDE_INPUT_SCHEMA", "WIDE_OUTPUT_SCHEMA"),
        "rollup.marts.write_marts": (
            "COMBINED_MART_INPUT_SCHEMA",
            "DIALSUP_MART_INPUT_SCHEMA",
        ),
    }

    for module_name, schema_names in expected_schemas.items():
        module = importlib.import_module(module_name)

        for schema_name in schema_names:
            assert isinstance(getattr(module, schema_name), pl.Schema)


def test_query_path_modules_do_not_define_private_helpers() -> None:
    rollup_root = Path(__file__).parents[1] / "src" / "rollup"
    query_paths = [
        rollup_root / "analysis.py",
        rollup_root / "pipeline.py",
        *sorted((rollup_root / "staging").glob("*.py")),
        *sorted((rollup_root / "intermediate").glob("*.py")),
        *sorted((rollup_root / "marts").glob("*.py")),
    ]
    violations: list[str] = []

    for path in query_paths:
        if path.name == "__init__.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name.startswith("_"):
                violations.append(f"{path.relative_to(rollup_root.parent)}:{node.lineno} {node.name}")

    assert violations == []


def test_schema_guard_catches_missing_required_column() -> None:
    frame = pl.DataFrame({"present": [1]})

    with pytest.raises(SchemaGuardError, match="missing columns"):
        require_columns(frame, pl.Schema({"present": pl.Int64, "missing": pl.String}))


def test_pipeline_inlines_intermediate_orchestration(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from rollup import pipeline

    calls: list[tuple[str, object]] = []
    stage_outputs: dict[str, tuple[str, ...]] = {}
    sources = SimpleNamespace(
        verisk_ylt="verisk_ylt",
        risklink_ylt="risklink_ylt",
        ep_summaries="ep_summaries",
        lobs="lobs",
        perils="perils",
        blending="blending",
        fx_rates="fx_rates",
        forecast_factors="forecast_factors",
        euws_factors="euws_factors",
    )
    config = SimpleNamespace(
        outputs=SimpleNamespace(staging_dir="staging", intermediate_dir="intermediate"),
    )

    def record(name: str, result: str):
        def transform(*args: object) -> str:
            calls.append((name, args))
            return result

        return transform

    def write_stage_frames(
        output_root: Path,
        section: str,
        frames: dict[str, object],
        config: object,
    ) -> tuple[Path, ...]:
        stage_outputs[section] = tuple(frames)
        return tuple(output_root / section / f"{name}.parquet" for name in frames)

    monkeypatch.setattr(pipeline, "load_sources", record("load_sources", sources))
    monkeypatch.setattr(pipeline, "normalize_ylt", record("normalize_ylt", "normalized"))
    monkeypatch.setattr(pipeline, "stage_ep_summaries", record("stage_ep_summaries", "staged_ep"))
    monkeypatch.setattr(pipeline, "build_enriched_ylt", record("build_enriched_ylt", "enriched"))
    monkeypatch.setattr(pipeline, "apply_blending", record("apply_blending", "blended"))
    monkeypatch.setattr(pipeline, "apply_fx", record("apply_fx", "fx_applied"))
    monkeypatch.setattr(pipeline, "apply_forecast", record("apply_forecast", "forecast_applied"))
    monkeypatch.setattr(pipeline, "apply_euws", record("apply_euws", "euws_applied"))
    monkeypatch.setattr(pipeline, "build_metric_long", record("build_metric_long", "combined"))
    monkeypatch.setattr(pipeline, "build_dialsup", record("build_dialsup", "dialsup"))
    monkeypatch.setattr(pipeline, "write_stage_frames", write_stage_frames)
    monkeypatch.setattr(pipeline, "write_marts", record("write_marts", {}))

    pipeline.run(tmp_path / "data", output_root=tmp_path / "output", config=config)

    assert calls == [
        ("load_sources", (tmp_path / "data",)),
        ("normalize_ylt", (sources,)),
        ("stage_ep_summaries", (sources,)),
        ("build_enriched_ylt", ("normalized", "staged_ep")),
        ("apply_blending", ("enriched", "blending")),
        ("apply_fx", ("blended", "fx_rates")),
        ("apply_forecast", ("fx_applied", "forecast_factors")),
        ("apply_euws", ("forecast_applied", "euws_factors")),
        ("build_metric_long", ("euws_applied",)),
        ("build_dialsup", ("combined",)),
        ("write_marts", (tmp_path / "output", "combined", "dialsup", config)),
    ]
    assert stage_outputs["intermediate"] == (
        "enriched_ylt",
        "blended_ylt",
        "fx_applied_ylt",
        "forecast_applied_ylt",
        "euws_applied_ylt",
    )


def test_config_loader_drives_counts_return_periods_and_outputs(tmp_path: Path) -> None:
    config_path = tmp_path / "rollup.toml"
    config_path.write_text(
        """
[analysis]
num_sims_verisk = 2
num_sims_risklink = 4
return_periods = [2]

[outputs]
write_stage_outputs = false
combined_file = "combined.parquet"
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.analysis.simulation_counts == {"verisk": 2, "risklink": 4}
    assert config.analysis.return_periods == (2,)
    assert config.outputs.write_stage_outputs is False
    assert config.outputs.combined_file == "combined.parquet"


def test_dataiku_run_writes_stage_mart_and_analysis_outputs(tmp_path: Path) -> None:
    from rollup.intermediate.apply_blending import BLENDED_YLT_SCHEMA
    from rollup.intermediate.apply_euws import EUWS_APPLIED_YLT_SCHEMA
    from rollup.intermediate.apply_forecast import FORECAST_APPLIED_YLT_SCHEMA
    from rollup.intermediate.apply_fx import FX_APPLIED_YLT_SCHEMA
    from rollup.intermediate.build_dialsup import DIALSUP_SCHEMA
    from rollup.intermediate.build_enriched_ylt import ENRICHED_YLT_OUTPUT_SCHEMA
    from rollup.intermediate.build_metric_long import METRIC_LONG_SCHEMA
    from rollup.marts.event_validation import EVENT_VALIDATION_SCHEMA
    from rollup.marts.wide import WIDE_OUTPUT_SCHEMA
    from rollup.staging.normalize_ylt import NORMALIZED_YLT_SCHEMA
    from rollup.staging.stage_ep_summaries import STAGED_EP_SUMMARIES_OUTPUT_SCHEMA

    data_root = _write_tiny_input(tmp_path)
    output_root = tmp_path / "output"
    config_path = tmp_path / "rollup.toml"
    config_path.write_text(
        """
[analysis]
num_sims_verisk = 2
num_sims_risklink = 4
return_periods = [2]

[outputs]
write_stage_outputs = true
""".strip(),
        encoding="utf-8",
    )

    result = run_rollup(data_root, output_root, config_path=config_path)

    assert result.outputs.stage_dir == output_root / "stages"
    normalized_ylt_path = output_root / "stages" / "staging" / "normalized_ylt.parquet"
    staged_ep_path = output_root / "stages" / "staging" / "staged_ep_summaries.parquet"
    assert normalized_ylt_path.is_file()
    assert staged_ep_path.is_file()
    intermediate_stage = output_root / "stages" / "intermediate"
    enriched_ylt_path = intermediate_stage / "enriched_ylt.parquet"
    blended_ylt_path = intermediate_stage / "blended_ylt.parquet"
    fx_applied_ylt_path = intermediate_stage / "fx_applied_ylt.parquet"
    forecast_applied_ylt_path = intermediate_stage / "forecast_applied_ylt.parquet"
    euws_applied_ylt_path = intermediate_stage / "euws_applied_ylt.parquet"
    assert enriched_ylt_path.is_file()
    assert blended_ylt_path.is_file()
    assert fx_applied_ylt_path.is_file()
    assert forecast_applied_ylt_path.is_file()
    assert euws_applied_ylt_path.is_file()
    assert not (intermediate_stage / "adjusted_ylt.parquet").exists()
    assert result.outputs.mts_combined.is_file()
    assert result.outputs.mts_wide.is_file()
    assert result.outputs.mts_dialsup.is_file()
    assert result.outputs.event_validation.is_file()
    assert result.ep_report_path == output_root / "analysis" / "ep_report.csv"
    assert result.ep_report_path.is_file()

    report = pl.read_csv(result.ep_report_path)
    assert set(report["return_period"].to_list()) == {0, 2}
    aal = report.filter((pl.col("ep_type") == "AAL") & (pl.col("base_model") == "verisk"))
    assert aal.filter(pl.col("metric") == "euws_override")["loss"].to_list() == [15.0]

    require_columns(pl.read_parquet(normalized_ylt_path), NORMALIZED_YLT_SCHEMA)
    require_columns(pl.read_parquet(staged_ep_path), STAGED_EP_SUMMARIES_OUTPUT_SCHEMA)
    require_columns(pl.read_parquet(enriched_ylt_path), ENRICHED_YLT_OUTPUT_SCHEMA)
    require_columns(pl.read_parquet(blended_ylt_path), BLENDED_YLT_SCHEMA)
    require_columns(pl.read_parquet(fx_applied_ylt_path), FX_APPLIED_YLT_SCHEMA)
    require_columns(pl.read_parquet(forecast_applied_ylt_path), FORECAST_APPLIED_YLT_SCHEMA)
    require_columns(pl.read_parquet(euws_applied_ylt_path), EUWS_APPLIED_YLT_SCHEMA)
    require_columns(pl.read_parquet(result.outputs.mts_combined), METRIC_LONG_SCHEMA)
    require_columns(pl.read_parquet(result.outputs.mts_wide), WIDE_OUTPUT_SCHEMA)
    require_columns(pl.read_parquet(result.outputs.mts_dialsup), DIALSUP_SCHEMA)
    require_columns(pl.read_parquet(result.outputs.event_validation), EVENT_VALIDATION_SCHEMA)


def _write_tiny_input(tmp_path: Path) -> Path:
    data_root = tmp_path / "data"
    _write_ylt(data_root)
    _write_ep_summaries(data_root)
    _write_seeds(data_root)
    return data_root


def _write_ylt(data_root: Path) -> None:
    verisk_dir = data_root / "ylt" / "verisk"
    risklink_dir = data_root / "ylt" / "risklink"
    verisk_dir.mkdir(parents=True)
    risklink_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "Analysis": ["EQ", "EQ"],
            "ExposureAttribute": ["Fine Art", "Fine Art"],
            "CatalogTypeCode": ["STC", "STC"],
            "EventID": [1, 2],
            "ModelCode": [7, 7],
            "YearID": [1, 2],
            "GroundUpLoss": [10.0, 20.0],
        }
    ).write_parquet(verisk_dir / "verisk.parquet")
    pl.DataFrame(
        {
            "anlsid": [9001, 9001],
            "yearid": [1, 2],
            "eventid": [1, 2],
            "loss": [40.0, 80.0],
        }
    ).write_parquet(risklink_dir / "risklink.parquet")


def _write_ep_summaries(data_root: Path) -> None:
    for vendor, analysis_id in {"verisk": "EQ", "risklink": "9001"}.items():
        folder = data_root / "ep_summaries" / vendor
        folder.mkdir(parents=True)
        pl.DataFrame(
            {
                "vendor": [vendor],
                "analysis_id": [analysis_id],
                "modelled_lob": ["Fine Art"],
                "modelled_peril": ["EQ"],
                "ep_type": ["AAL"],
                "return_period": [0],
                "loss": [1.0],
            }
        ).write_csv(folder / f"{vendor}.long.csv")


def _write_seeds(data_root: Path) -> None:
    seeds = data_root / "seeds"
    seeds.mkdir(parents=True)
    pl.DataFrame(
        {
            "modelled_lob": ["Fine Art"],
            "rollup_lob": ["Fine Art"],
            "class": ["ART"],
            "office": ["London"],
            "currency": ["GBP"],
        }
    ).write_csv(seeds / "lobs.csv")
    pl.DataFrame(
        {
            "modelled_peril": ["EQ"],
            "rollup_peril": ["Earthquake"],
            "region_peril_id": [205],
            "selection_priority": [1],
            "is_dialsup": [1],
        }
    ).write_csv(seeds / "perils.csv")
    pl.DataFrame(
        {
            "RegionPerilID": [205],
            "AIRBlend": [1.0],
            "RMSBlend": [0.5],
        }
    ).write_csv(seeds / "blending_factors.csv")
    pl.DataFrame(
        {
            "currency_code": ["GBP"],
            "rate": [1.0],
        }
    ).write_csv(seeds / "fx_rates.csv")
    pl.DataFrame(
        {
            "class": ["ART"],
            "office": ["London"],
            "forecast_date": ["2026-01-01"],
            "factor": [1.0],
        }
    ).write_csv(seeds / "forecast_factors.csv")
    pl.DataFrame(
        {
            "event_id": [1, 2],
            "factor": [1.0, 1.0],
        }
    ).write_csv(seeds / "euws_rate_factors.csv")
