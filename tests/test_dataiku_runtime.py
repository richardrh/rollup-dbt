from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from rollup.api import run_rollup
from rollup.config import load_config
from rollup.metrics import final_main_metric


def test_import_surface() -> None:
    import rollup
    from rollup.api import run_rollup as api_run_rollup
    from rollup.pipeline import run
    from rollup.staging import load_sources, normalize_ylt, stage_ep_summaries

    assert rollup.run_rollup is api_run_rollup
    assert not hasattr(rollup, "validate_rollup_inputs")
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
        "rollup.marts.write_marts": "write_marts",
        "rollup.marts.wide": "wide",
        "rollup.marts.fanouts": "write_fanouts",
    }

    for module_name, member_name in expected_members.items():
        module = importlib.import_module(module_name)

        assert callable(getattr(module, member_name))


def test_load_sources_reports_missing_input_files(tmp_path: Path) -> None:
    from rollup.staging.load_sources import load_sources

    data_root = tmp_path / "data"

    with pytest.raises(FileNotFoundError, match="no parquet files found"):
        load_sources(data_root)


def test_load_risklink_flood_events_maps_min_occurrence_date_to_ordinal_day(tmp_path: Path) -> None:
    from rollup.columns import Col
    from rollup.staging.load_sources import load_risklink_flood_events

    validation = tmp_path / "data" / "seeds" / "validation"
    validation.mkdir(parents=True)
    pl.DataFrame(
        {
            "ModelEventID": [10, 10, 11],
            "RegionPerilID": [216, 216, 217],
            "ModelOccurrenceDate": ["2026-02-03", "2026-01-05", "2026-12-31"],
        }
    ).with_columns(
        pl.col("ModelOccurrenceDate").str.to_date()
    ).write_parquet(validation / "risklink_flood22_model_events.parquet")

    result = load_risklink_flood_events(tmp_path / "data").collect().sort(Col.event_id)

    assert result.columns == [Col.event_id, Col.region_peril_id, Col.risklink_event_day]
    assert result.rows() == [(10, 216, 5), (11, 217, 365)]


def test_pipeline_inlines_intermediate_orchestration(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from rollup import pipeline

    calls: list[tuple[str, object]] = []
    stage_outputs: dict[str, tuple[str, ...]] = {}
    sources = SimpleNamespace(
        verisk_ylt="verisk_ylt",
        risklink_ylt="risklink_ylt",
        verisk_events="verisk_events",
        risklink_flood_events="risklink_flood_events",
        ep_summaries="ep_summaries",
        lobs="lobs",
        perils="perils",
        blending="blending",
        fx_rates="fx_rates",
        forecast_factors="forecast_factors",
        euws_factors="euws_factors",
        euws_overrides="euws_overrides",
    )
    blending_config = SimpleNamespace(vendor_years={"verisk": 123, "risklink": 456})
    config = SimpleNamespace(
        blending=blending_config,
        outputs=SimpleNamespace(staging_dir="staging", intermediate_dir="intermediate"),
        fx=SimpleNamespace(target_currency="GBP"),
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
    monkeypatch.setattr(pipeline, "main_fanout_source", record("main_fanout_source", "main_fanout"))
    monkeypatch.setattr(pipeline, "dialsup_fanout_source", record("dialsup_fanout_source", "dialsup_fanout"))
    monkeypatch.setattr(pipeline, "_write_stage_frames", write_stage_frames)
    monkeypatch.setattr(pipeline, "write_marts", record("write_marts", {}))

    pipeline.run(tmp_path / "data", output_root=tmp_path / "output", config=config)

    assert calls == [
        ("load_sources", (tmp_path / "data",)),
        ("normalize_ylt", (sources,)),
        ("stage_ep_summaries", (sources,)),
        ("build_enriched_ylt", ("normalized", "staged_ep")),
        (
            "apply_blending",
            ("enriched", "staged_ep", "blending", blending_config),
        ),
        ("apply_fx", ("blended", "fx_rates", "GBP")),
        ("apply_forecast", ("fx_applied", "forecast_factors")),
        ("apply_euws", ("forecast_applied", "verisk_events", "euws_factors", "euws_overrides")),
        ("build_metric_long", ("euws_applied", "GBP")),
        ("build_dialsup", ("euws_applied", "GBP")),
        ("main_fanout_source", ("euws_applied", "GBP")),
        ("dialsup_fanout_source", ("euws_applied", "GBP")),
        (
            "write_marts",
            (
                tmp_path / "output",
                "combined",
                "dialsup",
                config,
                "risklink_flood_events",
                "main_fanout",
                "dialsup_fanout",
            ),
        ),
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
return_periods = [2]

[vendor_years]
verisk = 7
risklink = 11

[blending]
uplift_factor_min = 0.25
uplift_factor_max = 4.0
target_points = [
  { ep_type = "AAL", return_period = 0 },
  { ep_type = "OEP", return_period = 5 },
]

[outputs]
write_stage_outputs = false
write_duckdb = true
combined_file = "combined.parquet"
duckdb_file = "custom-rollup.duckdb"

[outputs.fanout_prefixes]
bespoke = "BespokeModel"

[fx]
target_currency = "usd"
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.analysis.simulation_counts == {"verisk": 7, "risklink": 11}
    assert config.analysis.return_periods == (2,)
    assert config.blending.vendor_years == {"verisk": 7, "risklink": 11}
    assert [(point.ep_type, point.return_period) for point in config.blending.target_points] == [
        ("AAL", 0),
        ("OEP", 5),
    ]
    assert config.blending.uplift_factor_min == 0.25
    assert config.blending.uplift_factor_max == 4.0
    assert config.outputs.write_stage_outputs is False
    assert config.outputs.write_duckdb is True
    assert config.outputs.combined_file == "combined.parquet"
    assert config.outputs.duckdb_file == "custom-rollup.duckdb"
    assert config.outputs.fanout_prefixes == {"bespoke": "BespokeModel"}
    assert config.outputs.duckdb_path(tmp_path / "output") == tmp_path / "output" / "custom-rollup.duckdb"
    assert config.fx.target_currency == "USD"


def test_analysis_report_uses_vendor_years_from_config_toml(tmp_path: Path) -> None:
    from rollup.analysis import build_ep_report

    output_root = tmp_path / "output"
    marts_dir = output_root / "marts"
    marts_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "forecast_date": ["2026-01-01", "2026-01-01"],
            "metric": [final_main_metric("GBP"), final_main_metric("GBP")],
            "base_model": ["custom_model", "custom_model"],
            "rollup_lob": ["Fine Art", "Fine Art"],
            "rollup_peril": ["Earthquake", "Earthquake"],
            "year_id": [1, 2],
            "loss": [10.0, 20.0],
        }
    ).write_parquet(marts_dir / "mts_tbl_ylt_combined_all_factors.parquet")
    config_path = tmp_path / "rollup.toml"
    config_path.write_text(
        """
[analysis]
return_periods = [2]

[vendor_years]
custom_model = 2
""".strip(),
        encoding="utf-8",
    )

    report = build_ep_report(output_root, config_path=config_path)

    aal = report.filter(pl.col("ep_type") == "AAL").select("loss").item()
    oep = report.filter(
        (pl.col("ep_type") == "OEP") & (pl.col("return_period") == 2)
    ).select("loss").item()
    assert aal == 15.0
    assert oep == 20.0


def test_dataiku_run_writes_stage_mart_and_analysis_outputs(tmp_path: Path) -> None:
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
write_duckdb = true
duckdb_file = "rollup.duckdb"
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
    assert not (output_root / "marts" / "mts_event_validation.parquet").exists()
    assert result.outputs.duckdb_file == output_root / "rollup.duckdb"
    assert result.outputs.duckdb_file.is_file()
    assert result.ep_report_path == output_root / "analysis" / "ep_report.csv"
    assert result.ep_report_path.is_file()

    report = pl.read_csv(result.ep_report_path)
    assert set(report["return_period"].to_list()) == {0, 2}
    aal = report.filter((pl.col("ep_type") == "AAL") & (pl.col("base_model") == "verisk"))
    assert aal.filter(pl.col("metric") == final_main_metric("GBP"))[
        "loss"
    ].to_list() == [15.0]

    combined = pl.read_parquet(result.outputs.mts_combined)
    wide = pl.read_parquet(result.outputs.mts_wide)
    dialsup = pl.read_parquet(result.outputs.mts_dialsup)
    assert {"metric", "loss", "forecast_date"}.issubset(combined.columns)
    assert "metric" not in wide.columns
    assert set(dialsup["metric"].unique().to_list()) == {"loss_dialsup_fx_gbp_forecast"}


def test_run_rollup_write_duckdb_true_returns_existing_duckdb_file(tmp_path: Path) -> None:
    data_root = _write_tiny_input(tmp_path)
    output_root = tmp_path / "output"
    config_path = tmp_path / "rollup.toml"
    config_path.write_text(
        """
[outputs]
write_duckdb = true
""".strip(),
        encoding="utf-8",
    )

    result = run_rollup(
        data_root,
        output_root,
        config_path=config_path,
        write_analysis=False,
    )

    assert result.outputs.duckdb_file == output_root / "rollup.duckdb"
    assert result.outputs.duckdb_file.is_file()


def test_run_rollup_write_duckdb_false_returns_none_and_skips_default_file(tmp_path: Path) -> None:
    data_root = _write_tiny_input(tmp_path)
    output_root = tmp_path / "output"
    config_path = tmp_path / "rollup.toml"
    config_path.write_text(
        """
[outputs]
write_duckdb = false
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_path)
    result = run_rollup(
        data_root,
        output_root,
        config_path=config_path,
        write_analysis=False,
    )

    assert config.outputs.write_duckdb is False
    assert result.outputs.duckdb_file is None
    assert not (output_root / "rollup.duckdb").exists()


def test_tiny_pipeline_expands_no_factor_hic_fa_uk_style_forecast_dates(tmp_path: Path) -> None:
    data_root = _write_tiny_input(tmp_path)
    seeds = data_root / "seeds"
    pl.DataFrame(
        {
            "lob_id": [1],
            "modelled_lob": ["Fine Art"],
            "rollup_lob": ["HIC_FA_UK"],
            "lob_type": ["property"],
            "cds_cat_class_name": ["FA"],
            "class": ["FA"],
            "office": ["UK"],
            "currency": ["GBP"],
        }
    ).write_csv(seeds / "lobs.csv")
    pl.DataFrame(
        {
            "class": ["PROP", "PROP", "PROP"],
            "office": ["US", "US", "US"],
            "office_iso2": ["US", "US", "US"],
            "forecast_date": ["2026-01-01", "2026-07-01", "2026-12-31"],
            "factor": [1.1, 1.2, 1.3],
        }
    ).write_csv(seeds / "forecast_factors.csv")

    result = run_rollup(data_root, tmp_path / "output")
    combined = pl.read_parquet(result.outputs.mts_combined)
    final_main = combined.filter(
        pl.col("metric") == final_main_metric("GBP")
    )

    assert final_main.select("forecast_date").unique().sort("forecast_date").to_series().to_list() == [
        "2026-01-01",
        "2026-07-01",
        "2026-12-31",
    ]
    assert final_main.select("forecast_date", "loss").group_by("forecast_date").sum().sort("forecast_date").rows() == [
        ("2026-01-01", 30.0),
        ("2026-07-01", 30.0),
        ("2026-12-31", 30.0),
    ]
    dialsup = pl.read_parquet(result.outputs.mts_dialsup)
    assert dialsup.group_by("forecast_date").agg(pl.col("loss").sum()).sort("forecast_date").rows() == [
        ("2026-01-01", 30.0),
        ("2026-07-01", 30.0),
        ("2026-12-31", 30.0),
    ]


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
    for vendor, analysis_id, oep_loss in [("verisk", "EQ", 100.0), ("risklink", "9001", 0.0)]:
        folder = data_root / "ep_summaries" / vendor
        folder.mkdir(parents=True)
        pl.DataFrame(
            {
                "vendor": [vendor, vendor],
                "analysis_id": [analysis_id, analysis_id],
                "modelled_lob": ["Fine Art", "Fine Art"],
                "modelled_peril": ["EQ", "EQ"],
                "ep_type": ["AAL", "OEP"],
                "return_period": [0, 1000],
                "loss": [1.0, oep_loss],
            }
        ).write_csv(folder / f"{vendor}.long.csv")


def _write_seeds(data_root: Path) -> None:
    seeds = data_root / "seeds"
    seeds.mkdir(parents=True)
    pl.DataFrame(
        {
            "lob_id": [1],
            "modelled_lob": ["Fine Art"],
            "rollup_lob": ["Fine Art"],
            "lob_type": ["property"],
            "cds_cat_class_name": ["ART"],
            "class": ["ART"],
            "office": ["London"],
            "currency": ["GBP"],
        }
    ).write_csv(seeds / "lobs.csv")
    pl.DataFrame(
        {
            "modelled_peril": ["EQ"],
            "rollup_peril": ["Earthquake"],
            "region": ["US"],
            "peril": ["EQ"],
            "region_peril_id": [205],
            "blend_subregion_peril_id": ["205a"],
            "base_model": ["verisk"],
            "selection_priority": [1],
            "is_dialsup": [1],
            "is_euws": [0],
        }
    ).write_csv(seeds / "perils.csv")
    pl.DataFrame(
        {
            "id": [1],
            "BlendSetID": [1],
            "RegionPerilID": [205],
            "RegionPeril": ["US_EQ"],
            "SubRegionPerilID": ["205a"],
            "SubRegionPeril": ["US_EQ"],
            "AIRBlend": [1.0],
            "RMSBlend": [0.5],
            "KatRiskBlend": [0.0],
            "DateCreated": ["2026-01-01"],
        }
    ).write_csv(seeds / "blending_factors.csv")
    pl.DataFrame(
        {
            "currency_code": ["GBP"],
            "target_currency": ["GBP"],
            "rate_date": ["2026-01-01"],
            "rate": [1.0],
        }
    ).write_csv(seeds / "fx_rates.csv")
    pl.DataFrame(
        {
            "class": ["ART"],
            "office": ["London"],
            "office_iso2": ["GB"],
            "forecast_date": ["2026-01-01"],
            "factor": [1.0],
        }
    ).write_csv(seeds / "forecast_factors.csv")
    pl.DataFrame(
        {
            "model_event_id": [101, 102],
            "occ_year": [1, 2],
            "factor": [1.0, 1.0],
        }
    ).write_csv(seeds / "euws_rate_factors.csv")
    validation = seeds / "validation"
    validation.mkdir()
    pl.DataFrame(
        {
            "EventID": [101, 102],
            "ModelID": [7, 7],
            "Event": [1, 2],
            "Year": [1, 2],
            "Day": [10, 20],
        }
    ).write_parquet(validation / "verisk_events.parquet")
    pl.DataFrame(
        {
            "ModelEventID": [1, 2],
            "RegionPerilID": [205, 205],
            "ModelOccurrenceDate": ["2026-01-10", "2026-01-20"],
        }
    ).with_columns(
        pl.col("ModelOccurrenceDate").str.to_date()
    ).write_parquet(validation / "risklink_flood22_model_events.parquet")
