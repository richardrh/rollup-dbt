from __future__ import annotations

import polars as pl

from rollup import cli
from rollup.columns import Col, RawCol
from rollup.pipeline import (
    EpSummaryValidationResult,
    SeedValidationResult,
    YltFrames,
    YltValidationResult,
    ensure_modelled_dimension_coverage,
    modelled_dimension_coverage_report,
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


def _cli_reports(*, valid: bool = True, coverage_error: bool = False) -> cli.ValidationReports:
    coverage_report = (
        pl.DataFrame({"severity": ["error"], "valid": [False]})
        if coverage_error
        else pl.DataFrame(schema={"severity": pl.String, "valid": pl.Boolean})
    )
    return cli.ValidationReports(
        inputs=object(),
        report=pl.DataFrame({"valid": [valid], "error": [None]}),
        coverage_report=coverage_report,
        ylt_loss_report=pl.DataFrame(),
    )


def test_run_command_prints_validation_reports_and_reuses_inputs(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    reports = _cli_reports()
    calls = {}

    monkeypatch.setattr(cli, "collect_validation_reports", lambda data_root: reports)

    def fake_run(data_root, *, output_root, debug, validation_inputs):
        calls["data_root"] = data_root
        calls["output_root"] = output_root
        calls["debug"] = debug
        calls["validation_inputs"] = validation_inputs

    monkeypatch.setattr(cli, "run", fake_run)

    output_root = tmp_path / "output"
    assert cli.run_command("data", output_root=output_root, debug=True) == 0

    captured = capsys.readouterr().out
    assert "Validation report" in captured
    assert "Modelled LOB/peril anti-join report" in captured
    assert "YLT loss validation summary" in captured
    assert calls == {
        "data_root": "data",
        "output_root": output_root,
        "debug": True,
        "validation_inputs": reports.inputs,
    }


def test_run_command_returns_nonzero_without_running_pipeline_on_validation_failure(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    monkeypatch.setattr(
        cli,
        "collect_validation_reports",
        lambda data_root: _cli_reports(valid=False),
    )

    def fail_run(*args, **kwargs):
        raise AssertionError("run should not be called when validation fails")

    monkeypatch.setattr(cli, "run", fail_run)

    assert cli.run_command("data", output_root=tmp_path / "output") == 1
    captured = capsys.readouterr().out
    assert "Validation report" in captured
    assert "Modelled LOB/peril anti-join report" in captured
    assert "YLT loss validation summary" in captured


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
