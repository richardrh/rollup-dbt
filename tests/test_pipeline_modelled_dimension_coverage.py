from __future__ import annotations

from pathlib import Path

import polars as pl

from rollup import cli
from rollup.columns import Col, RawCol
from rollup.config import BlendingConfig, BlendingTargetPoint, RollupConfig
from rollup.pipeline import (
    apply_ep_blending_to_ylt,
    calculate_ep_blending_targets,
    EpSummaryValidationResult,
    PipelineValidationInputs,
    SeedValidationResult,
    YltFrames,
    YltValidationResult,
    ensure_modelled_dimension_coverage,
    input_ylt_aal_by_lob_peril_summary,
    _add_rank_columns,
    join_ep_summaries,
    modelled_dimension_coverage_report,
    stage_ep_summaries,
    write_parquet_with_log,
)


def _valid_report() -> pl.DataFrame:
    return pl.DataFrame({"valid": [True], "error": [None]})


def _seed_result() -> SeedValidationResult:
    return SeedValidationResult(
        frames={
            "lobs.csv": pl.DataFrame({Col.modelled_lob: ["LOB_A", "LOB_UNUSED"]}),
            "perils.csv": pl.DataFrame({Col.modelled_peril: ["PERIL_A", "PERIL_UNUSED"]}),
        },
        report=_valid_report(),
    )


def _ylt_result() -> YltValidationResult:
    return YltValidationResult(
        frames=YltFrames(
            verisk=pl.DataFrame(
                {
                    RawCol.CatalogTypeCode: ["STC", "STC", "NON_STC"],
                    RawCol.ExposureAttribute: ["LOB_A", "LOB_MISSING", "LOB_NON_STC"],
                    RawCol.Analysis: ["PERIL_A", "PERIL_MISSING", "PERIL_NON_STC"],
                }
            ).lazy(),
            risklink=pl.DataFrame({RawCol.anlsid: [1]}).lazy(),
        ),
        report=_valid_report(),
    )


def _ep_summary_result() -> EpSummaryValidationResult:
    return EpSummaryValidationResult(
        frame=pl.DataFrame(
            {
                Col.modelled_lob: ["LOB_A", "EP_LOB_MISSING"],
                Col.modelled_peril: ["PERIL_A", "EP_PERIL_MISSING"],
            }
        ).lazy(),
        report=_valid_report(),
    )


def _summary_seed_result() -> SeedValidationResult:
    return SeedValidationResult(
        frames={
            "lobs.csv": pl.DataFrame(
                {
                    Col.modelled_lob: ["LOB_A", "LOB_B", "LOB_RISK"],
                    Col.rollup_lob: ["Rollup A", "Rollup B", "Rollup Risk"],
                    Col.cds_cat_class_name: ["Class A", "Class B", "Class Risk"],
                    Col.class_: ["A", "B", "R"],
                    Col.office: ["London", "London", "London"],
                    Col.currency: ["GBP", "GBP", "GBP"],
                }
            ),
            "perils.csv": pl.DataFrame(
                {
                    Col.modelled_peril: ["PERIL_A", "PERIL_B", "PERIL_RISK"],
                    Col.rollup_peril: ["Rollup EQ", "Rollup WS", "Rollup FL"],
                    "region": ["Region", "Region", "Region"],
                    "peril": ["EQ", "WS", "FL"],
                    Col.region_peril_id: [101, 102, 103],
                    Col.blend_subregion_peril_id: ["101", "102", "103"],
                    Col.base_model: ["verisk", "verisk", "risklink"],
                    Col.selection_priority: [1, 1, 1],
                    Col.is_dialsup: [1, 1, 1],
                    Col.is_euws: [0, 1, 0],
                }
            ),
        },
        report=_valid_report(),
    )


def _empty_risklink_ylt() -> pl.LazyFrame:
    return pl.DataFrame(schema={RawCol.anlsid: pl.Int64, RawCol.loss: pl.Float64}).lazy()


def _empty_verisk_ylt() -> pl.LazyFrame:
    return pl.DataFrame(
        schema={
            RawCol.CatalogTypeCode: pl.String,
            RawCol.ExposureAttribute: pl.String,
            RawCol.Analysis: pl.String,
            RawCol.GroundUpLoss: pl.Float64,
        }
    ).lazy()


def _summary_inputs(
    *,
    verisk: pl.LazyFrame | None = None,
    risklink: pl.LazyFrame | None = None,
    ep_rows: dict[str, list[object]] | None = None,
) -> PipelineValidationInputs:
    ep_frame = pl.DataFrame(
        ep_rows
        or {
            Col.vendor: ["risklink"],
            Col.analysis_id: ["9001"],
            Col.modelled_lob: ["LOB_RISK"],
            Col.modelled_peril: ["PERIL_RISK"],
            Col.ep_type: ["AAL"],
            Col.return_period: [0],
            Col.loss: [1.0],
        }
    ).lazy()
    return PipelineValidationInputs(
        seeds=_summary_seed_result(),
        ylts=YltValidationResult(
            frames=YltFrames(
                verisk=verisk if verisk is not None else _empty_verisk_ylt(),
                risklink=risklink if risklink is not None else _empty_risklink_ylt(),
            ),
            report=_valid_report(),
        ),
        ep_summaries=EpSummaryValidationResult(frame=ep_frame, report=_valid_report()),
        coverage_report=pl.DataFrame(schema={"severity": pl.String, "valid": pl.Boolean}),
    )


def _peril_selection_seed_result() -> SeedValidationResult:
    return SeedValidationResult(
        frames={
            "lobs.csv": pl.DataFrame(
                {
                    Col.modelled_lob: ["LOB"],
                    Col.rollup_lob: ["Property"],
                    Col.cds_cat_class_name: ["Class"],
                    Col.class_: ["CLASS"],
                    Col.office: ["Office"],
                    Col.currency: ["GBP"],
                }
            ),
            "perils.csv": pl.DataFrame(
                {
                    Col.modelled_peril: ["HIGH", "LOW", "DIAL_A", "DIAL_B"],
                    Col.rollup_peril: ["Europe_FL", "Europe_FL", "Europe_FL", "Europe_FL"],
                    "region": ["Europe"] * 4,
                    "peril": ["FL"] * 4,
                    Col.region_peril_id: [216] * 4,
                    Col.blend_subregion_peril_id: ["216b"] * 4,
                    Col.base_model: ["risklink"] * 4,
                    Col.selection_priority: [20, 10, 30, 40],
                    Col.is_dialsup: [0, 0, 1, 1],
                    Col.is_euws: [0, 0, 0, 0],
                }
            ),
        },
        report=_valid_report(),
    )


def _peril_selection_ep_summary_result() -> EpSummaryValidationResult:
    return EpSummaryValidationResult(
        frame=pl.DataFrame(
            {
                Col.vendor: ["risklink"] * 4,
                Col.analysis_id: ["1", "2", "3", "4"],
                Col.modelled_lob: ["LOB"] * 4,
                Col.modelled_peril: ["HIGH", "LOW", "DIAL_A", "DIAL_B"],
                Col.ep_type: ["AAL"] * 4,
                Col.return_period: [0] * 4,
                Col.loss: [1.0, 2.0, 3.0, 4.0],
            }
        ).lazy(),
        report=_valid_report(),
    )


def test_modelled_dimension_coverage_report_returns_only_input_missing_errors() -> None:
    report = modelled_dimension_coverage_report(
        _seed_result(),
        _ylt_result(),
        _ep_summary_result(),
    )

    rows = {
        (
            row["severity"],
            row["direction"],
            row["source_group"],
            row["dimension"],
            row["value"],
        )
        for row in report.iter_rows(named=True)
    }

    assert (
        "error",
        "input_missing_from_seed",
        "verisk_ylt",
        Col.modelled_lob,
        "LOB_MISSING",
    ) in rows
    assert (
        "error",
        "input_missing_from_seed",
        "verisk_ylt",
        Col.modelled_peril,
        "PERIL_MISSING",
    ) in rows
    assert (
        "error",
        "input_missing_from_seed",
        "ep_summaries",
        Col.modelled_lob,
        "EP_LOB_MISSING",
    ) in rows
    assert (
        "error",
        "input_missing_from_seed",
        "ep_summaries",
        Col.modelled_peril,
        "EP_PERIL_MISSING",
    ) in rows
    assert {row[0] for row in rows} == {"error"}
    assert {row[1] for row in rows} == {"input_missing_from_seed"}
    assert "LOB_UNUSED" not in set(report["value"])
    assert "PERIL_UNUSED" not in set(report["value"])
    assert "LOB_NON_STC" not in set(report["value"])


def test_main_selection_chooses_lowest_selection_priority() -> None:
    staged = stage_ep_summaries(
        _peril_selection_ep_summary_result(),
        _peril_selection_seed_result(),
    )

    selected = staged.selected.select(Col.modelled_peril).collect().to_series().to_list()

    assert selected == ["LOW"]


def test_dialsup_selection_keeps_all_is_dialsup_candidates() -> None:
    staged = stage_ep_summaries(
        _peril_selection_ep_summary_result(),
        _peril_selection_seed_result(),
    )

    selected = (
        staged.selected_dialsup.select(Col.modelled_peril)
        .collect()
        .to_series()
        .sort()
        .to_list()
    )

    assert selected == ["DIAL_A", "DIAL_B"]


def test_blending_joins_weights_by_blend_subregion_peril_id() -> None:
    seeds = SeedValidationResult(
        frames={
            "blending_factors.csv": pl.DataFrame(
                {
                    RawCol.RegionPerilID: [216, 216],
                    RawCol.SubRegionPerilID: ["216a", "216b"],
                    RawCol.SubRegionPeril: ["unused", "selected"],
                    RawCol.AIRBlend: [1.0, 0.25],
                    RawCol.RMSBlend: [0.0, 0.75],
                }
            )
        },
        report=_valid_report(),
    )
    selection_seeds = _peril_selection_seed_result()
    joined = join_ep_summaries(
        stage_ep_summaries(
            EpSummaryValidationResult(
                frame=pl.DataFrame(
                    {
                        Col.vendor: ["verisk", "risklink"],
                        Col.analysis_id: ["AIR", "RMS"],
                        Col.modelled_lob: ["LOB", "LOB"],
                        Col.modelled_peril: ["LOW", "LOW"],
                        Col.ep_type: ["AAL", "AAL"],
                        Col.return_period: [0, 0],
                        Col.loss: [100.0, 200.0],
                    }
                ).lazy(),
                report=_valid_report(),
            ),
            SeedValidationResult(
                frames={
                    "lobs.csv": selection_seeds.frames["lobs.csv"],
                    "perils.csv": selection_seeds.frames["perils.csv"].filter(
                        pl.col(Col.modelled_peril) == "LOW"
                    ),
                },
                report=_valid_report(),
            ),
        )
    )

    blended = calculate_ep_blending_targets(joined, seeds).blended.collect()

    assert blended.item(0, Col.blend_subregion_peril_id) == "216b"
    assert blended.item(0, Col.sub_region_peril) == "selected"
    assert blended.item(0, Col.risklink_blended_contribution) == 150.0
    assert blended.item(0, Col.verisk_blended_contribution) == 25.0
    assert blended.item(0, Col.target_loss) == 175.0
    assert blended.item(0, Col.base_model) == "risklink"


def test_apply_ep_blending_to_ylt_retains_blend_diagnostics() -> None:
    seeds = SeedValidationResult(
        frames={
            "blending_factors.csv": pl.DataFrame(
                {
                    RawCol.RegionPerilID: [216],
                    RawCol.SubRegionPerilID: ["216b"],
                    RawCol.SubRegionPeril: ["selected"],
                    RawCol.AIRBlend: [0.25],
                    RawCol.RMSBlend: [0.75],
                }
            )
        },
        report=_valid_report(),
    )
    selection_seeds = _peril_selection_seed_result()
    targets = calculate_ep_blending_targets(
        join_ep_summaries(
            stage_ep_summaries(
                EpSummaryValidationResult(
                    frame=pl.DataFrame(
                        {
                            Col.vendor: ["verisk", "risklink"],
                            Col.analysis_id: ["AIR", "RMS"],
                            Col.modelled_lob: ["LOB", "LOB"],
                            Col.modelled_peril: ["LOW", "LOW"],
                            Col.ep_type: ["AAL", "AAL"],
                            Col.return_period: [0, 0],
                            Col.loss: [100.0, 200.0],
                        }
                    ).lazy(),
                    report=_valid_report(),
                ),
                SeedValidationResult(
                    frames={
                        "lobs.csv": selection_seeds.frames["lobs.csv"],
                        "perils.csv": selection_seeds.frames["perils.csv"].filter(
                            pl.col(Col.modelled_peril) == "LOW"
                        ),
                    },
                    report=_valid_report(),
                ),
            )
        ),
        seeds,
    )
    ylt = pl.DataFrame(
        {
            Col.rollup_lob: ["Property"],
            Col.rollup_peril: ["Europe_FL"],
            Col.region_peril_id: [216],
            Col.blend_subregion_peril_id: ["216b"],
            Col.rp_bucket: [0],
            Col.base_model: ["risklink"],
            Col.loss: [10.0],
            Col.metric: ["original"],
        }
    ).lazy()

    blended_ylt = apply_ep_blending_to_ylt(ylt, targets).collect()

    assert Col.risklink_blended_contribution in blended_ylt.columns
    assert Col.verisk_blended_contribution in blended_ylt.columns
    assert Col.uplift_factor_on_base_model in blended_ylt.columns
    assert blended_ylt.item(0, Col.risklink_blended_contribution) == 150.0
    assert blended_ylt.item(0, Col.verisk_blended_contribution) == 25.0
    assert blended_ylt.item(0, Col.uplift_factor_on_base_model) == 0.875
    assert blended_ylt.item(0, Col.loss) == 8.75


def test_blending_falls_back_to_base_model_loss_when_counterparty_missing() -> None:
    seeds = SeedValidationResult(
        frames={
            "blending_factors.csv": pl.DataFrame(
                {
                    RawCol.RegionPerilID: [216],
                    RawCol.SubRegionPerilID: ["216b"],
                    RawCol.SubRegionPeril: ["selected"],
                    RawCol.AIRBlend: [0.25],
                    RawCol.RMSBlend: [0.75],
                }
            )
        },
        report=_valid_report(),
    )
    joined = join_ep_summaries(
        stage_ep_summaries(
            EpSummaryValidationResult(
                frame=pl.DataFrame(
                    {
                        Col.vendor: ["risklink"],
                        Col.analysis_id: ["RMS"],
                        Col.modelled_lob: ["LOB"],
                        Col.modelled_peril: ["LOW"],
                        Col.ep_type: ["AAL"],
                        Col.return_period: [0],
                        Col.loss: [200.0],
                    }
                ).lazy(),
                report=_valid_report(),
            ),
            SeedValidationResult(
                frames={
                    "lobs.csv": _peril_selection_seed_result().frames["lobs.csv"],
                    "perils.csv": _peril_selection_seed_result().frames["perils.csv"].filter(pl.col(Col.modelled_peril) == "LOW"),
                },
                report=_valid_report(),
            ),
        )
    )

    blended = calculate_ep_blending_targets(joined, seeds).blended.collect()

    assert blended.height == 1
    assert blended.item(0, Col.target_loss) == 200.0
    assert blended.item(0, Col.uplift_factor_on_base_model) == 1.0
    assert blended.item(0, Col.risklink_blended_contribution) == 200.0
    assert blended.item(0, Col.verisk_blended_contribution) == 0.0


def test_blending_uses_configured_target_points_caps_and_vendor_years() -> None:
    config = RollupConfig(
        blending=BlendingConfig(
            vendor_years={"verisk": 4, "risklink": 8},
            target_points=(BlendingTargetPoint("AAL", 0), BlendingTargetPoint("OEP", 2)),
            uplift_factor_min=0.5,
            uplift_factor_max=2.0,
        )
    )
    ranked = _add_rank_columns(
        pl.DataFrame(
            {
                Col.vendor: ["verisk", "verisk"],
                Col.modelled_lob: ["LOB", "LOB"],
                Col.rollup_peril: ["PERIL", "PERIL"],
                Col.loss: [100.0, 50.0],
            }
        ).lazy(),
        config,
    ).collect()
    assert ranked.sort(Col.loss, descending=True)[Col.rp].to_list() == [4.0, 2.0]
    assert ranked.sort(Col.loss, descending=True)[Col.rp_bucket].to_list() == [2, 2]

    # Configured caps are applied to target/base ratios.
    seeds = SeedValidationResult(
        frames={
            "blending_factors.csv": pl.DataFrame(
                {
                    RawCol.RegionPerilID: [1],
                    RawCol.SubRegionPerilID: ["1"],
                    RawCol.SubRegionPeril: ["x"],
                    RawCol.AIRBlend: [10.0],
                    RawCol.RMSBlend: [10.0],
                }
            )
        },
        report=_valid_report(),
    )
    joined = join_ep_summaries(
        stage_ep_summaries(
            EpSummaryValidationResult(
                frame=pl.DataFrame(
                    {
                        Col.vendor: ["verisk", "risklink"],
                        Col.analysis_id: ["AIR", "RMS"],
                        Col.modelled_lob: ["LOB", "LOB"],
                        Col.modelled_peril: ["LOW", "LOW"],
                        Col.ep_type: ["OEP", "OEP"],
                        Col.return_period: [2, 2],
                        Col.loss: [100.0, 1000.0],
                    }
                ).lazy(),
                report=_valid_report(),
            ),
            SeedValidationResult(
                frames={
                    "lobs.csv": _peril_selection_seed_result().frames["lobs.csv"],
                    "perils.csv": _peril_selection_seed_result()
                    .frames["perils.csv"]
                    .filter(pl.col(Col.modelled_peril) == "LOW")
                    .with_columns(
                        pl.lit(1).alias(Col.region_peril_id),
                        pl.lit("1").alias(Col.blend_subregion_peril_id),
                    ),
                },
                report=_valid_report(),
            ),
        )
    )
    blended = calculate_ep_blending_targets(joined, seeds, config).blended.collect()
    assert blended.item(0, Col.uplift_factor_on_base_model) == 2.0


def test_input_ylt_aal_summary_computes_verisk_raw_aal_sorted_descending() -> None:
    inputs = _summary_inputs(
        verisk=pl.DataFrame(
            {
                RawCol.CatalogTypeCode: ["STC", "STC", "STC", "NON_STC"],
                RawCol.ExposureAttribute: ["LOB_A", "LOB_A", "LOB_B", "LOB_B"],
                RawCol.Analysis: ["PERIL_A", "PERIL_A", "PERIL_B", "PERIL_B"],
                RawCol.GroundUpLoss: [15_000.0, 5_000.0, 10_000.0, 90_000.0],
            }
        ).lazy()
    )

    report = input_ylt_aal_by_lob_peril_summary(inputs)

    rows = report.filter(pl.col(Col.vendor) == "verisk").iter_rows(named=True)
    assert list(rows) == [
        {
            Col.vendor: "verisk",
            Col.rollup_lob: "Rollup A",
            Col.rollup_peril: "Rollup EQ",
            Col.modelled_lob: "LOB_A",
            Col.modelled_peril: "PERIL_A",
            Col.row_count: 2,
            Col.loss_sum: 20_000.0,
            "simulation_count": 10_000,
            "raw_aal": 2.0,
        },
        {
            Col.vendor: "verisk",
            Col.rollup_lob: "Rollup B",
            Col.rollup_peril: "Rollup WS",
            Col.modelled_lob: "LOB_B",
            Col.modelled_peril: "PERIL_B",
            Col.row_count: 1,
            Col.loss_sum: 10_000.0,
            "simulation_count": 10_000,
            "raw_aal": 1.0,
        },
    ]


def test_input_ylt_aal_summary_does_not_duplicate_risklink_losses_by_ep_return_period() -> None:
    inputs = _summary_inputs(
        risklink=pl.DataFrame(
            {
                RawCol.anlsid: [9001, 9001],
                RawCol.loss: [100.0, 200.0],
            }
        ).lazy(),
        ep_rows={
            Col.vendor: ["risklink", "risklink", "risklink"],
            Col.analysis_id: ["9001", "9001", "9001"],
            Col.modelled_lob: ["LOB_RISK", "LOB_RISK", "LOB_RISK"],
            Col.modelled_peril: ["PERIL_RISK", "PERIL_RISK", "PERIL_RISK"],
            Col.ep_type: ["AAL", "OEP", "OEP"],
            Col.return_period: [0, 200, 1000],
            Col.loss: [1.0, 10.0, 20.0],
        },
    )

    report = input_ylt_aal_by_lob_peril_summary(inputs)

    risklink = report.filter(pl.col(Col.vendor) == "risklink")
    assert risklink.height == 1
    assert risklink.item(0, Col.row_count) == 2
    assert risklink.item(0, Col.loss_sum) == 300.0
    assert risklink.item(0, "simulation_count") == 100_000
    assert risklink.item(0, "raw_aal") == 0.003


def test_ensure_modelled_dimension_coverage_raises_only_on_errors() -> None:
    warning_only = pl.DataFrame(
        {
            "severity": ["warning"],
            "valid": [False],
        }
    )
    ensure_modelled_dimension_coverage(warning_only)

    error_report = pl.DataFrame(
        {
            "severity": ["error"],
            "valid": [False],
        }
    )
    try:
        ensure_modelled_dimension_coverage(error_report)
    except ValueError as exc:
        assert "rollup validate" in str(exc)
    else:
        raise AssertionError("expected coverage errors to raise")


def test_validate_command_returns_nonzero_for_coverage_errors(monkeypatch) -> None:
    monkeypatch.setattr(
        cli,
        "collect_validation_reports",
        lambda data_root: _cli_reports(coverage_error=True),
    )

    assert cli.validate_command("data") == 1


def test_validate_parser_accepts_report_dir_only_on_validate_command() -> None:
    args = cli.build_parser().parse_args(
        ["validate", "--report-dir", "output/validation"]
    )

    assert args.command == "validate"
    assert args.report_dir == Path("output/validation")


def test_validate_command_prints_input_ylt_aal_report(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "collect_validation_reports", lambda data_root: _cli_reports())

    assert cli.validate_command("data") == 0
    captured = capsys.readouterr().out
    assert "Input YLT AAL by LOB/peril summary" in captured


def test_validate_command_writes_csv_reports_and_preserves_console_output(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    monkeypatch.setattr(cli, "collect_validation_reports", lambda data_root: _cli_reports())
    report_dir = tmp_path / "validation"

    assert cli.validate_command("data", report_dir=report_dir) == 0

    expected_files = {
        "validation_report.csv",
        "modelled_lob_peril_anti_join_report.csv",
        "ylt_loss_validation_summary.csv",
        "input_ylt_aal_by_lob_peril_summary.csv",
    }
    for filename in expected_files:
        output_path = report_dir / filename
        assert output_path.is_file()
        assert output_path.read_text(encoding="utf-8")

    captured = capsys.readouterr().out
    assert "Validation report" in captured
    assert "Modelled LOB/peril anti-join report" in captured
    assert "YLT loss validation summary" in captured
    assert "Input YLT AAL by LOB/peril summary" in captured
    assert f"Validation CSV reports written to {report_dir}" in captured


def test_validate_command_report_dir_preserves_validation_exit_code(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(
        cli,
        "collect_validation_reports",
        lambda data_root: _cli_reports(coverage_error=True),
    )

    report_dir = tmp_path / "validation"
    assert cli.validate_command("data", report_dir=report_dir) == 1
    assert (report_dir / "validation_report.csv").is_file()


def test_validate_command_returns_nonzero_when_report_writing_fails(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    monkeypatch.setattr(cli, "collect_validation_reports", lambda data_root: _cli_reports())

    def fail_write(*args, **kwargs):
        raise OSError("permission denied")

    monkeypatch.setattr(cli, "write_validation_csv_reports", fail_write)

    report_dir = tmp_path / "validation"
    assert cli.validate_command("data", report_dir=report_dir) == 1
    captured = capsys.readouterr()
    assert "Failed to write validation CSV reports" in captured.err
    assert "permission denied" in captured.err


def _cli_reports(*, valid: bool = True, coverage_error: bool = False) -> cli.ValidationReports:
    coverage_report = (
        pl.DataFrame({"severity": ["error"], "valid": [False]})
        if coverage_error
        else pl.DataFrame(schema={"severity": pl.String, "valid": pl.Boolean})
    )
    return cli.ValidationReports(
        data_root=Path("data"),
        is_valid=valid and not coverage_error,
        validation_report=pl.DataFrame({"valid": [valid], "error": [None]}),
        coverage_report=coverage_report,
        ylt_loss_report=pl.DataFrame({"vendor": ["verisk"], "loss_sum": [1.0]}),
        input_ylt_aal_report=pl.DataFrame(
            {Col.vendor: ["verisk"], "raw_aal": [0.0001]}
        ),
    )


def test_run_command_prints_validation_reports_from_api_result(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    reports = _cli_reports()
    calls = {}

    def fake_run_rollup(data_root, *, output_root, debug, validation_callback):
        calls["data_root"] = data_root
        calls["output_root"] = output_root
        calls["debug"] = debug
        validation_callback(reports)
        return type(
            "RunResult",
            (),
            {"ep_report_path": output_root / "analysis" / "ep_report.csv"},
        )()

    monkeypatch.setattr(cli, "run_rollup", fake_run_rollup)

    output_root = tmp_path / "output"
    assert cli.run_command("data", output_root=output_root, debug=True) == 0

    captured = capsys.readouterr().out
    assert "Validation report" in captured
    assert "Modelled LOB/peril anti-join report" in captured
    assert "YLT loss validation summary" in captured
    assert "Input YLT AAL by LOB/peril summary" in captured
    assert calls == {
        "data_root": "data",
        "output_root": output_root,
        "debug": True,
    }


def test_run_command_returns_nonzero_without_running_pipeline_on_validation_failure(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    reports = _cli_reports(valid=False)

    def fail_validation(*args, **kwargs):
        kwargs["validation_callback"](reports)
        raise cli.RollupValidationError(reports)

    monkeypatch.setattr(cli, "run_rollup", fail_validation)

    assert cli.run_command("data", output_root=tmp_path / "output") == 1
    captured = capsys.readouterr().out
    assert "Validation report" in captured
    assert "Modelled LOB/peril anti-join report" in captured
    assert "YLT loss validation summary" in captured
    assert "Input YLT AAL by LOB/peril summary" in captured


def test_write_parquet_with_log_emits_one_completion_record(tmp_path, caplog) -> None:
    output_path = tmp_path / "output.parquet"
    caplog.set_level("INFO", logger="rollup.pipeline")

    write_parquet_with_log(pl.DataFrame({"value": [1, 2]}), output_path)

    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "rollup.pipeline"
    ]
    assert len(messages) == 1
    assert messages[0].startswith(f"wrote output={output_path} rows=2 elapsed=")
    assert not any(message.startswith("writing output=") for message in messages)


def test_write_parquet_with_log_sinks_lazy_frame_with_unknown_rows(tmp_path, caplog) -> None:
    output_path = tmp_path / "nested" / "output.parquet"
    caplog.set_level("INFO", logger="rollup.pipeline")

    write_parquet_with_log(pl.DataFrame({"value": [1, 2]}).lazy(), output_path)

    assert pl.read_parquet(output_path).to_dict(as_series=False) == {"value": [1, 2]}
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "rollup.pipeline"
    ]
    assert len(messages) == 1
    assert messages[0].startswith(f"wrote output={output_path} rows=-1 elapsed=")
