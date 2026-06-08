from __future__ import annotations

import importlib
from pathlib import Path

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
        "rollup.intermediate.apply_adjustments": "apply_adjustments",
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


def test_schema_guard_catches_missing_required_column() -> None:
    frame = pl.DataFrame({"present": [1]})

    with pytest.raises(SchemaGuardError, match="missing columns"):
        require_columns(frame, pl.Schema({"present": pl.Int64, "missing": pl.String}))


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
    assert (output_root / "stages" / "staging" / "normalized_ylt.parquet").is_file()
    assert (output_root / "stages" / "intermediate" / "adjusted_ylt.parquet").is_file()
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
