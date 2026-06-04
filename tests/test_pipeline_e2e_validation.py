from __future__ import annotations

import shutil
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

from rollup.cli import validate_command
from rollup.columns import Col, FanoutCol, RawCol
from rollup.api import run_rollup
from rollup.pipeline import (
    EpSummaryValidationResult,
    SeedValidationResult,
    YltFrames,
    YltValidationResult,
    calculate_dialsup,
    dialsup_peril_selection_report,
    enrich_ylt_with_ep_summaries,
    load_validated_ep_summary_frames,
    load_validated_seed_frames,
    load_validated_ylt_frames,
    modelled_dimension_coverage_report,
    normalize_ylt,
    run,
    stage_ep_summaries,
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
            Col.is_dialsup: [1],
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
            RawCol.GroundUpLoss: [10.0] * row_count,
        }
    ).write_parquet(output_dir / "new_lob.parquet")


def _write_risklink_ylt(data_root: Path) -> None:
    output_dir = data_root / "ylt" / "risklink"
    output_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            RawCol.yearid: [1],
            RawCol.eventid: [1],
            RawCol.anlsid: [9001],
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


def _peril_selection_seed_result(*, is_dialsup: list[int]) -> SeedValidationResult:
    return SeedValidationResult(
        frames={
            "lobs.csv": pl.DataFrame(
                {
                    "lob_id": [1],
                    Col.modelled_lob: ["LOB_A"],
                    Col.rollup_lob: ["ROLLUP_LOB"],
                    "lob_type": ["test"],
                    Col.cds_cat_class_name: ["Test Class"],
                    Col.office: ["TestOffice"],
                    Col.class_: ["TEST"],
                    Col.currency: ["GBP"],
                }
            ),
            "perils.csv": pl.DataFrame(
                {
                    Col.modelled_peril: ["PERIL_BASE", "PERIL_ADJ"],
                    Col.rollup_peril: ["Test_WS", "Test_WS"],
                    "region": ["Test", "Test"],
                    "peril": ["WS", "WS"],
                    Col.region_peril_id: [205, 205],
                    Col.selection_priority: [2, 1],
                    Col.is_dialsup: is_dialsup,
                }
            ),
            "fx_rates.csv": pl.DataFrame(
                {
                    RawCol.currency_code: ["GBP"],
                    Col.target_currency: ["GBP"],
                    RawCol.rate_date: [date(2026, 1, 1)],
                    RawCol.rate: [1.0],
                }
            ),
            "forecast_factors.csv": pl.DataFrame(
                {
                    Col.class_: ["TEST"],
                    Col.office: ["TestOffice"],
                    "office_iso2": ["TT"],
                    Col.forecast_date: [date(2026, 1, 1)],
                    RawCol.factor: [1.0],
                }
            ),
        },
        report=pl.DataFrame({"valid": [True], "error": [None]}),
    )


def _peril_selection_ep_result() -> EpSummaryValidationResult:
    return EpSummaryValidationResult(
        frame=pl.DataFrame(
            {
                Col.vendor: ["verisk", "verisk"],
                Col.analysis_id: ["PERIL_BASE", "PERIL_ADJ"],
                Col.modelled_lob: ["LOB_A", "LOB_A"],
                Col.modelled_peril: ["PERIL_BASE", "PERIL_ADJ"],
                Col.ep_type: ["AAL", "AAL"],
                Col.return_period: [0, 0],
                Col.loss: [1.0, 1.0],
            }
        ).lazy(),
        report=pl.DataFrame({"valid": [True], "error": [None]}),
    )


def test_main_selection_still_uses_lowest_selection_priority() -> None:
    staged = stage_ep_summaries(
        _peril_selection_ep_result(),
        _peril_selection_seed_result(is_dialsup=[1, 0]),
    )

    assert staged.selected.select(Col.modelled_peril).unique().collect().to_series().to_list() == [
        "PERIL_ADJ"
    ]
    assert staged.selected_dialsup.select(Col.modelled_peril).unique().collect().to_series().to_list() == [
        "PERIL_BASE"
    ]


def test_dialsup_uses_is_dialsup_peril_instead_of_main_priority_selection() -> None:
    seeds = _peril_selection_seed_result(is_dialsup=[1, 0])
    staged = stage_ep_summaries(_peril_selection_ep_result(), seeds)
    normalized = normalize_ylt(
        YltValidationResult(
            frames=YltFrames(
                verisk=pl.DataFrame(
                    {
                        RawCol.Analysis: ["PERIL_BASE", "PERIL_ADJ"],
                        RawCol.ExposureAttribute: ["LOB_A", "LOB_A"],
                        RawCol.CatalogTypeCode: ["STC", "STC"],
                        RawCol.EventID: [1, 2],
                        RawCol.ModelCode: [1, 1],
                        RawCol.YearID: [1, 1],
                        RawCol.GroundUpLoss: [10.0, 99.0],
                    }
                ).lazy(),
                risklink=pl.DataFrame(
                    schema={
                        RawCol.anlsid: pl.Int64,
                        RawCol.yearid: pl.Int64,
                        RawCol.eventid: pl.Int64,
                        RawCol.loss: pl.Float64,
                    }
                ).lazy(),
            ),
            report=pl.DataFrame({"valid": [True], "error": [None]}),
        )
    )

    main = enrich_ylt_with_ep_summaries(normalized, staged).combined.collect()
    dialsup_selected = enrich_ylt_with_ep_summaries(
        normalized,
        staged,
        use_dialsup_selection=True,
    ).combined
    dialsup = calculate_dialsup(
        dialsup_selected.with_columns(
            pl.lit("verisk").alias(Col.base_model),
            pl.lit("original").alias(Col.metric),
            pl.lit(1).cast(pl.Int64).alias(Col.rnk),
            pl.lit(10_000.0).alias(Col.rp),
            pl.lit(1000).alias(Col.rp_bucket),
        ),
        pl.DataFrame(
            {
                Col.event_id: [1, 2],
                Col.year_id: [1, 1],
                Col.model_code: [1, 1],
                Col.model_event_id: [101, 102],
                Col.event_day: [1, 1],
            }
        ).lazy(),
        seeds,
    ).collect()

    assert main.select(Col.modelled_peril).unique().to_series().to_list() == ["PERIL_ADJ"]
    assert dialsup.filter(pl.col(Col.metric) == "dialsup_gbp_forecast").select(
        Col.modelled_peril, Col.loss
    ).rows() == [("PERIL_BASE", 10.0)]


@pytest.mark.parametrize(
    ("is_dialsup", "expected_count"),
    [([0, 0], 0), ([1, 1], 2)],
)
def test_dialsup_selection_validation_requires_exactly_one_active_candidate(
    is_dialsup: list[int],
    expected_count: int,
) -> None:
    report = dialsup_peril_selection_report(
        _peril_selection_seed_result(is_dialsup=is_dialsup),
        _peril_selection_ep_result(),
    )

    assert report.height == 1
    assert report.item(0, "dimension") == Col.is_dialsup
    assert report.item(0, "count") == expected_count
    assert "exactly one is_dialsup=1" in report.item(0, "error")


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


def test_seed_extra_column_still_fails_strict_validation(tmp_path: Path) -> None:
    data_root = _write_minimal_data_root(tmp_path)
    pl.read_csv(data_root / "seeds" / "business" / "lobs.csv").with_columns(
        pl.lit("unexpected").alias("extra_column")
    ).write_csv(data_root / "seeds" / "business" / "lobs.csv")

    report = load_validated_seed_frames(data_root).report.filter(
        pl.col("filename") == "lobs.csv"
    )

    assert not report.item(0, "valid")
    assert "unexpected columns" in report.item(0, "error")
    assert "extra_column" in report.item(0, "error")


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


def test_raw_ylt_extra_columns_validate(tmp_path: Path) -> None:
    data_root = _write_minimal_data_root(tmp_path)
    pl.read_parquet(data_root / "ylt" / "risklink" / "risklink_ylt.parquet").with_columns(
        pl.lit("vendor diagnostic").alias("unused_vendor_column")
    ).write_parquet(data_root / "ylt" / "risklink" / "risklink_ylt.parquet")

    report = load_validated_ylt_frames(data_root).report.filter(
        (pl.col("vendor") == "risklink") & (pl.col("filename") == "risklink_ylt.parquet")
    )

    assert report.item(0, "valid")
    assert validate_command(data_root) == 0


def test_optional_ylt_column_wrong_dtype_fails_when_present(tmp_path: Path) -> None:
    data_root = _write_minimal_data_root(tmp_path)
    pl.read_parquet(data_root / "ylt" / "risklink" / "risklink_ylt.parquet").with_columns(
        pl.lit("not-a-float").alias("p_value")
    ).write_parquet(data_root / "ylt" / "risklink" / "risklink_ylt.parquet")

    report = load_validated_ylt_frames(data_root).report.filter(
        (pl.col("vendor") == "risklink") & (pl.col("filename") == "risklink_ylt.parquet")
    )

    assert not report.item(0, "valid")
    assert "dtype mismatches" in report.item(0, "error")
    assert "p_value" in report.item(0, "error")


def test_minimal_raw_ylt_contracts_validate_and_run(tmp_path: Path) -> None:
    data_root = _write_minimal_data_root(tmp_path, verisk_row_count=1_000)

    ylt_report = load_validated_ylt_frames(data_root).report

    assert ylt_report.filter(~pl.col("valid")).is_empty()
    assert validate_command(data_root) == 0
    assert run(data_root, output_root=tmp_path / "output").marts.frames


def test_risklink_ylt_analysis_id_missing_from_ep_summary_blocks_validation_and_run(
    tmp_path: Path,
) -> None:
    data_root = _write_minimal_data_root(tmp_path)
    pl.read_parquet(data_root / "ylt" / "risklink" / "risklink_ylt.parquet").with_columns(
        pl.lit(9999).cast(pl.Int64).alias(RawCol.anlsid)
    ).write_parquet(data_root / "ylt" / "risklink" / "risklink_ylt.parquet")

    seeds = load_validated_seed_frames(data_root)
    ylts = load_validated_ylt_frames(data_root)
    ep_summaries = load_validated_ep_summary_frames(data_root)
    from rollup.pipeline import build_semantic_validation_report

    report = build_semantic_validation_report(seeds, ylts, ep_summaries, data_root)
    failures = report.filter(
        (pl.col("check_name") == "risklink_analysis_coverage")
        & (pl.col("value") == "9999")
    )

    assert failures.height == 1
    assert failures.item(0, "field") == RawCol.anlsid
    assert "RiskLink YLT analysis id is missing" in failures.item(0, "error")
    assert validate_command(data_root) == 1
    with pytest.raises(ValueError, match="RiskLink YLT analysis id is missing"):
        run(data_root)


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

    ylt_original = result.intermediate.frames["ylt_original"].collect()
    assert ylt_original.filter(pl.col(Col.modelled_lob) == "NEW_LOB").height == 1_000

    ylt_long = result.marts.frames["ylt_long"]
    n_metrics = ylt_long.select(Col.metric).unique().height
    n_forecast = ylt_long.select(Col.forecast_date).unique().height

    main_fanout = result.marts.frames["main_fanout"].collect()
    assert main_fanout.height == 1_000
    assert main_fanout.select(pl.col(FanoutCol.ModelGrossLoss).sum()).item() == pytest.approx(
        10_000.0
    )

    written_main_fanout = pl.read_parquet(
        output_root / "marts" / "HiscoAIR_202601_euws_override.parquet"
    )
    assert written_main_fanout.height == 1_000
    assert written_main_fanout.select(
        pl.col(FanoutCol.ModelGrossLoss).sum()
    ).item() == pytest.approx(10_000.0)

    assert (output_root / "mts_tbl_ylt_combined_all_factors.parquet").is_file()
    assert (output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet").is_file()
    assert (output_root / "debug" / "mts_ylt_long.parquet").is_file()

    wide_mts = pl.read_parquet(output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet")
    assert "euws_override_202601_loss" in wide_mts.columns
    assert "dialsup_gbp_forecast_202601_loss" in wide_mts.columns
    assert wide_mts.select(pl.col("euws_override_202601_loss").sum()).item() == pytest.approx(
        10_000.0
    )
    assert wide_mts.select(pl.col("dialsup_gbp_forecast_202601_loss").sum()).item() == pytest.approx(
        10_000.0
    )


def test_run_rollup_log_file_includes_checkpoint_row_counts(tmp_path: Path) -> None:
    data_root = _write_minimal_data_root(
        tmp_path,
        modelled_lob="NEW_LOB",
        modelled_peril="NEW_PERIL",
        verisk_row_count=1_000,
    )
    output_root = tmp_path / "output"
    log_file = tmp_path / "logs" / "run.log"

    run_rollup(data_root, output_root, write_analysis=False, log_file=log_file)

    log_text = log_file.read_text(encoding="utf-8")
    for checkpoint in (
        "ylt_original",
        "ylt_ranked",
        "ylt_blended",
        "ylt_dialsup",
        "ylt_gbp",
        "ylt_gbp_forecast",
        "ylt_euws",
        "ylt_euws_override",
    ):
        assert f"checkpoint={checkpoint} rows=" in log_text
