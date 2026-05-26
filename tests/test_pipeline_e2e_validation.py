from __future__ import annotations

import shutil
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

from rollup.cli import validate_command
from rollup.columns import Col, FanoutCol, RawCol
from rollup.pipeline import (
    load_validated_ep_summary_frames,
    load_validated_seed_frames,
    load_validated_ylt_frames,
    modelled_dimension_coverage_report,
    run,
    ylt_loss_validation_summary,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _copy_schema_files(data_root: Path) -> None:
    for relative_path in (
        Path("seeds/schema.yaml"),
        Path("ep_summaries/schema.yaml"),
        Path("ylt/schema.yaml"),
    ):
        target = data_root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO_ROOT / "data" / relative_path, target)


def _write_seed_csvs(data_root: Path, *, modelled_lob: str, modelled_peril: str) -> None:
    (data_root / "seeds" / "business").mkdir(parents=True, exist_ok=True)
    (data_root / "seeds" / "vor").mkdir(parents=True, exist_ok=True)
    (data_root / "seeds" / "adjustments").mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "lob_id": [1],
            Col.modelled_lob: [modelled_lob],
            Col.rollup_lob: [modelled_lob],
            "lob_type": ["test"],
            Col.cds_cat_class_name: ["Test Class"],
            Col.office: ["TestOffice"],
            Col.class_: ["TEST"],
            Col.currency: ["GBP"],
        }
    ).write_csv(data_root / "seeds" / "business" / "lobs.csv")

    pl.DataFrame(
        {
            Col.modelled_peril: [modelled_peril],
            Col.rollup_peril: ["Test_EQ"],
            "region": ["Test"],
            "peril": ["EQ"],
            Col.region_peril_id: [205],
            Col.selection_priority: [1],
        }
    ).write_csv(data_root / "seeds" / "business" / "perils.csv")

    pl.DataFrame(
        {
            "id": [1],
            "BlendSetID": [1],
            RawCol.RegionPerilID: [205],
            "RegionPeril": ["Test Earthquake"],
            RawCol.SubRegionPerilID: ["205a"],
            RawCol.SubRegionPeril: ["Test"],
            RawCol.AIRBlend: [1.0],
            RawCol.RMSBlend: [0.0],
            "KatRiskBlend": [0.0],
            "DateCreated": [datetime(2026, 1, 1)],
        }
    ).write_csv(data_root / "seeds" / "vor" / "blending_factors.csv")

    pl.DataFrame(
        {
            RawCol.currency_code: ["GBP"],
            Col.target_currency: ["GBP"],
            RawCol.rate_date: [date(2026, 1, 1)],
            RawCol.rate: [1.0],
        }
    ).write_csv(data_root / "seeds" / "vor" / "fx_rates.csv")

    pl.DataFrame(
        {
            Col.class_: ["TEST"],
            Col.office: ["TestOffice"],
            "office_iso2": ["TT"],
            Col.forecast_date: [date(2026, 1, 1)],
            RawCol.factor: [1.0],
        }
    ).write_csv(data_root / "seeds" / "vor" / "forecast_factors.csv")

    pl.DataFrame(
        {
            Col.model_event_id: [1],
            RawCol.occ_year: [1],
            RawCol.factor: [1.0],
        }
    ).write_csv(data_root / "seeds" / "vor" / "euws_rate_factors.csv")

    pl.DataFrame(
        {
            Col.rollup_lob: [modelled_lob],
            RawCol.max_rank: [100],
            RawCol.factor: [1.0],
        }
    ).write_csv(data_root / "seeds" / "adjustments" / "euws_rank_overrides.csv")


def _write_ep_summaries(
    data_root: Path,
    *,
    modelled_lob: str,
    modelled_peril: str,
) -> None:
    rows_by_vendor = {
        "verisk": "verisk-analysis",
        "risklink": "9001",
    }
    for vendor, analysis_id in rows_by_vendor.items():
        output_dir = data_root / "ep_summaries" / vendor
        output_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                Col.vendor: [vendor, vendor, vendor],
                Col.analysis_id: [analysis_id, analysis_id, analysis_id],
                Col.modelled_lob: [modelled_lob, modelled_lob, modelled_lob],
                Col.modelled_peril: [modelled_peril, modelled_peril, modelled_peril],
                Col.ep_type: ["AAL", "OEP", "OEP"],
                Col.return_period: [0, 200, 1000],
                Col.loss: [1.0, 10.0, 10.0],
            }
        ).write_csv(output_dir / f"{vendor}_ep_summary.long.csv")


def _write_verisk_ylt(
    data_root: Path,
    *,
    modelled_lob: str,
    modelled_peril: str,
    row_count: int,
) -> None:
    output_dir = data_root / "ylt" / "verisk"
    output_dir.mkdir(parents=True, exist_ok=True)
    event_ids = list(range(1, row_count + 1))
    pl.DataFrame(
        {
            RawCol.Analysis: [modelled_peril] * row_count,
            RawCol.ExposureAttribute: [modelled_lob] * row_count,
            RawCol.CatalogTypeCode: ["STC"] * row_count,
            RawCol.EventID: event_ids,
            RawCol.ModelCode: [1] * row_count,
            RawCol.YearID: [1] * row_count,
            "PerilSetCode": [1] * row_count,
            RawCol.GroundUpLoss: [10.0] * row_count,
            "GrossLoss": [10.0] * row_count,
            "NetOfPreCatLoss": [10.0] * row_count,
            "filename": ["new_lob.parquet"] * row_count,
        }
    ).write_parquet(output_dir / "new_lob.parquet")


def _write_risklink_ylt(data_root: Path) -> None:
    output_dir = data_root / "ylt" / "risklink"
    output_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            RawCol.yearid: [1],
            RawCol.eventid: [1],
            "p_value": [0.01],
            RawCol.anlsid: [9001],
            "meanloss": [10.0],
            "stddev": [0.0],
            "expvalue": [10.0],
            RawCol.loss: [10.0],
        }
    ).write_parquet(output_dir / "risklink_ylt.parquet")


def _write_validation_catalogues(data_root: Path, *, row_count: int) -> None:
    output_dir = data_root / "seeds" / "validation"
    output_dir.mkdir(parents=True, exist_ok=True)
    event_ids = list(range(1, row_count + 1))
    pl.DataFrame(
        {
            RawCol.EventID: [100_000 + event_id for event_id in event_ids],
            RawCol.ModelID: [1] * row_count,
            RawCol.Event: event_ids,
            RawCol.Year: [1] * row_count,
            RawCol.Day: [1] * row_count,
        }
    ).write_parquet(output_dir / "verisk_events.parquet")

    pl.DataFrame(
        {
            "ModelEventID": [1],
            RawCol.RegionPerilID: [205],
            RawCol.ModelOccurrenceDate: [date(2026, 1, 1)],
        }
    ).write_parquet(output_dir / "risklink_flood22_model_events.parquet")


def _write_minimal_data_root(
    tmp_path: Path,
    *,
    modelled_lob: str = "TEST_LOB",
    modelled_peril: str = "TEST_PERIL",
    verisk_row_count: int = 10,
) -> Path:
    data_root = tmp_path / "data"
    _copy_schema_files(data_root)
    _write_seed_csvs(data_root, modelled_lob=modelled_lob, modelled_peril=modelled_peril)
    _write_ep_summaries(
        data_root,
        modelled_lob=modelled_lob,
        modelled_peril=modelled_peril,
    )
    _write_verisk_ylt(
        data_root,
        modelled_lob=modelled_lob,
        modelled_peril=modelled_peril,
        row_count=verisk_row_count,
    )
    _write_risklink_ylt(data_root)
    _write_validation_catalogues(data_root, row_count=verisk_row_count)
    return data_root


def _coverage_report(data_root: Path) -> pl.DataFrame:
    seeds = load_validated_seed_frames(data_root)
    ylts = load_validated_ylt_frames(data_root)
    ep_summaries = load_validated_ep_summary_frames(data_root)
    return modelled_dimension_coverage_report(seeds, ylts, ep_summaries)


def _assert_coverage_failure(
    report: pl.DataFrame,
    *,
    dimension: str,
    value: str,
) -> None:
    failures = report.filter(
        (pl.col("dimension") == dimension)
        & (pl.col("value") == value)
        & (pl.col("severity") == "error")
    )
    assert failures.height == 1
    assert value == failures.item(0, "value")
    assert dimension == failures.item(0, "dimension")


def test_seed_schema_mismatch_fails_validation(tmp_path: Path) -> None:
    data_root = _write_minimal_data_root(tmp_path)
    pl.DataFrame(
        {
            "lob_id": [1],
            Col.modelled_lob: ["TEST_LOB"],
            Col.rollup_lob: ["TEST_LOB"],
            "lob_type": ["test"],
            Col.cds_cat_class_name: ["Test Class"],
            Col.office: ["TestOffice"],
            Col.class_: ["TEST"],
        }
    ).write_csv(data_root / "seeds" / "business" / "lobs.csv")

    report = load_validated_seed_frames(data_root).report.filter(
        pl.col("filename") == "lobs.csv"
    )

    assert not report.item(0, "valid")
    assert "currency" in report.item(0, "error")
    assert validate_command(data_root) == 1


def test_ep_summary_schema_mismatch_fails_validation(tmp_path: Path) -> None:
    data_root = _write_minimal_data_root(tmp_path)
    pl.read_csv(data_root / "ep_summaries" / "verisk" / "verisk_ep_summary.long.csv").with_columns(
        pl.lit("unexpected").alias("extra_column")
    ).write_csv(data_root / "ep_summaries" / "verisk" / "verisk_ep_summary.long.csv")

    report = load_validated_ep_summary_frames(data_root).report.filter(
        pl.col("filename") == "verisk_ep_summary.long.csv"
    )

    assert not report.item(0, "valid")
    assert "unexpected columns" in report.item(0, "error")
    assert "extra_column" in report.item(0, "error")
    assert validate_command(data_root) == 1


def test_ylt_schema_mismatch_fails_validation(tmp_path: Path) -> None:
    data_root = _write_minimal_data_root(tmp_path)
    pl.read_parquet(data_root / "ylt" / "verisk" / "new_lob.parquet").with_columns(
        pl.col(RawCol.EventID).cast(pl.String)
    ).write_parquet(data_root / "ylt" / "verisk" / "new_lob.parquet")

    report = load_validated_ylt_frames(data_root).report.filter(
        (pl.col("vendor") == "verisk") & (pl.col("filename") == "new_lob.parquet")
    )

    assert not report.item(0, "valid")
    assert "dtype mismatches" in report.item(0, "error")
    assert RawCol.EventID in report.item(0, "error")
    assert validate_command(data_root) == 1


def test_ep_summary_unknown_lob_fails_semantic_validation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = _write_minimal_data_root(tmp_path)
    _write_ep_summaries(
        data_root,
        modelled_lob="UNKNOWN_LOB",
        modelled_peril="TEST_PERIL",
    )

    report = _coverage_report(data_root)

    _assert_coverage_failure(
        report,
        dimension=Col.modelled_lob,
        value="UNKNOWN_LOB",
    )
    assert validate_command(data_root) == 1
    assert "modelled_lob" in capsys.readouterr().out
    with pytest.raises(ValueError, match="modelled LOB/peril coverage validation failed"):
        run(data_root)


def test_ep_summary_unknown_peril_fails_semantic_validation(tmp_path: Path) -> None:
    data_root = _write_minimal_data_root(tmp_path)
    _write_ep_summaries(
        data_root,
        modelled_lob="TEST_LOB",
        modelled_peril="UNKNOWN_PERIL",
    )

    report = _coverage_report(data_root)

    _assert_coverage_failure(
        report,
        dimension=Col.modelled_peril,
        value="UNKNOWN_PERIL",
    )
    assert validate_command(data_root) == 1


def test_verisk_ylt_unknown_lob_fails_semantic_validation(tmp_path: Path) -> None:
    data_root = _write_minimal_data_root(tmp_path)
    _write_verisk_ylt(
        data_root,
        modelled_lob="UNKNOWN_YLT_LOB",
        modelled_peril="TEST_PERIL",
        row_count=10,
    )

    report = _coverage_report(data_root)

    _assert_coverage_failure(
        report,
        dimension=Col.modelled_lob,
        value="UNKNOWN_YLT_LOB",
    )
    assert validate_command(data_root) == 1


def test_verisk_ylt_unknown_peril_fails_semantic_validation(tmp_path: Path) -> None:
    data_root = _write_minimal_data_root(tmp_path)
    _write_verisk_ylt(
        data_root,
        modelled_lob="TEST_LOB",
        modelled_peril="UNKNOWN_YLT_PERIL",
        row_count=10,
    )

    report = _coverage_report(data_root)

    _assert_coverage_failure(
        report,
        dimension=Col.modelled_peril,
        value="UNKNOWN_YLT_PERIL",
    )
    assert validate_command(data_root) == 1


def test_new_lob_with_matching_ep_summary_and_verisk_ylt_runs_end_to_end(
    tmp_path: Path,
) -> None:
    data_root = _write_minimal_data_root(
        tmp_path,
        modelled_lob="NEW_LOB",
        modelled_peril="NEW_PERIL",
        verisk_row_count=1_000,
    )

    assert validate_command(data_root) == 0
    ylt_loss_report = ylt_loss_validation_summary(data_root).filter(
        (pl.col("vendor") == "verisk") & (pl.col("filename") == "new_lob.parquet")
    )
    assert ylt_loss_report.item(0, "loss_sum") == pytest.approx(10_000.0)
    assert ylt_loss_report.item(0, "scaled_loss") == pytest.approx(1.0)

    output_root = tmp_path / "output"
    result = run(data_root, output_root=output_root, debug=True)

    base_model = result.intermediate.frames["ylt_base_model"].collect()
    assert base_model.filter(pl.col(Col.modelled_lob) == "NEW_LOB").height == 1_000

    mart = result.marts.frames["ylt_combined_all_factors"].collect()
    assert mart.filter(pl.col(Col.rollup_lob) == "NEW_LOB").height == 1_000

    main_fanout = result.marts.frames["main_fanout"].collect()
    assert main_fanout.height == 1_000
    assert main_fanout.select(pl.col(FanoutCol.ModelGrossLoss).sum()).item() == pytest.approx(
        10_000.0
    )

    written_main_fanout = pl.read_parquet(
        output_root / "marts" / "HiscoAIR_202601_main.parquet"
    )
    assert written_main_fanout.height == 1_000
    assert written_main_fanout.select(
        pl.col(FanoutCol.ModelGrossLoss).sum()
    ).item() == pytest.approx(10_000.0)

    assert (output_root / "mts_tbl_ylt_combined_all_factors.parquet").is_file()
    assert (output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet").is_file()
    assert (output_root / "debug" / "mts_ylt_combined_all_factors.parquet").is_file()

    wide_mts = pl.read_parquet(output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet")
    assert "main_202601_loss" in wide_mts.columns
    assert "dialsup_202601_loss" in wide_mts.columns
    assert wide_mts.height == mart.height
    assert wide_mts.select(pl.col("main_202601_loss").sum()).item() == pytest.approx(
        10_000.0
    )
    assert wide_mts.select(pl.col("dialsup_202601_loss").sum()).item() == pytest.approx(
        10_000.0
    )
