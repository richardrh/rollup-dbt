from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from rollup import api


def _source_report(*, valid: bool = True, error: str | None = None) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "filename": ["input.csv"],
            "valid": [valid],
            "error": [error],
        }
    )


def _coverage_report(*, severity: str | None = None) -> pl.DataFrame:
    if severity is None:
        return pl.DataFrame(schema={"severity": pl.String})
    return pl.DataFrame({"severity": [severity]})


def _validation_inputs(*, valid: bool = True, coverage_severity: str | None = None):
    return SimpleNamespace(
        seeds=SimpleNamespace(report=_source_report()),
        ylts=SimpleNamespace(report=_source_report(valid=valid, error=None if valid else "bad")),
        ep_summaries=SimpleNamespace(report=_source_report()),
        coverage_report=_coverage_report(severity=coverage_severity),
    )


def _patch_validation_helpers(monkeypatch: pytest.MonkeyPatch, inputs) -> None:
    monkeypatch.setattr(api, "load_pipeline_validation_inputs", lambda data_root: inputs)
    monkeypatch.setattr(
        api,
        "ylt_loss_validation_summary",
        lambda data_root: pl.DataFrame({"valid": [True]}),
    )
    monkeypatch.setattr(
        api,
        "input_ylt_aal_by_lob_peril_summary",
        lambda validation_inputs: pl.DataFrame({"raw_aal": [1.0]}),
    )


def test_validate_rollup_inputs_returns_structured_reports(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    inputs = _validation_inputs()
    _patch_validation_helpers(monkeypatch, inputs)

    result = api.validate_rollup_inputs(tmp_path / "data")

    assert result.data_root == tmp_path / "data"
    assert result.is_valid is True
    assert result.validation_report.get_column("source_group").to_list() == [
        "seeds",
        "ylt",
        "ep_summaries",
    ]
    assert result.ylt_loss_report.get_column("valid").to_list() == [True]
    assert result.input_ylt_aal_report.get_column("raw_aal").to_list() == [1.0]
    assert set(result.report_frames()) == {
        "validation_report.csv",
        "modelled_lob_peril_anti_join_report.csv",
        "ylt_loss_validation_summary.csv",
        "input_ylt_aal_by_lob_peril_summary.csv",
    }


def test_validate_rollup_inputs_writes_reports_when_report_dir_is_passed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    inputs = _validation_inputs()
    _patch_validation_helpers(monkeypatch, inputs)
    report_dir = tmp_path / "validation"

    result = api.validate_rollup_inputs(tmp_path / "data", report_dir=report_dir)

    expected_files = {
        "validation_report.csv",
        "modelled_lob_peril_anti_join_report.csv",
        "ylt_loss_validation_summary.csv",
        "input_ylt_aal_by_lob_peril_summary.csv",
    }
    assert set(result.report_frames()) == expected_files
    for filename in expected_files:
        output_path = report_dir / filename
        assert output_path.is_file()
        assert output_path.read_text(encoding="utf-8")


def test_validation_result_write_reports_returns_written_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    inputs = _validation_inputs()
    _patch_validation_helpers(monkeypatch, inputs)
    result = api.validate_rollup_inputs(tmp_path / "data")

    written_paths = result.write_reports(tmp_path / "reports")

    assert written_paths == {
        filename: tmp_path / "reports" / filename
        for filename in result.report_frames()
    }
    assert all(path.is_file() for path in written_paths.values())


def test_validation_result_raises_with_structured_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    inputs = _validation_inputs(valid=False, coverage_severity="error")
    _patch_validation_helpers(monkeypatch, inputs)

    result = api.validate_rollup_inputs(tmp_path / "data")

    assert result.is_valid is False
    with pytest.raises(api.RollupValidationError) as exc_info:
        result.raise_for_errors()
    assert exc_info.value.validation is result
    assert "1 invalid file(s), 1 coverage error(s)" in str(exc_info.value)


def test_run_rollup_returns_dataiku_friendly_output_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "output"
    inputs = _validation_inputs()
    _patch_validation_helpers(monkeypatch, inputs)
    events: list[str] = []

    def fake_run(
        data_root_arg: Path,
        *,
        output_root: Path,
        debug: bool,
        validation_inputs,
    ) -> object:
        assert data_root_arg == data_root
        assert debug is True
        assert validation_inputs is inputs
        events.append("run")
        (output_root / "marts").mkdir(parents=True)
        (output_root / "marts" / "HiscoAIR_202601_main.parquet").write_bytes(b"")
        (output_root / "mts_tbl_ylt_combined_all_factors.parquet").write_bytes(b"")
        (output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet").write_bytes(b"")
        (output_root / "mts_tbl_ylt_dialsup.parquet").write_bytes(b"")
        (output_root / "mts_event_validation.parquet").write_bytes(b"")
        return object()

    def fake_write_ep_report(output_root_arg: Path) -> Path:
        events.append("write_ep_report")
        path = output_root_arg / "analysis" / "ep_report.csv"
        path.parent.mkdir(parents=True)
        path.write_text("ok\n", encoding="utf-8")
        return path

    monkeypatch.setattr(api, "run", fake_run)
    monkeypatch.setattr(api, "write_ep_report", fake_write_ep_report)

    result = api.run_rollup(data_root, output_root, debug=True)

    assert events == ["run", "write_ep_report"]
    assert result.data_root == data_root
    assert result.output_root == output_root
    assert result.ep_report_path == output_root / "analysis" / "ep_report.csv"
    assert result.outputs.mts_wide == output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet"
    assert result.outputs.mts_wide.exists()
    assert result.outputs.mart_files == (output_root / "marts" / "HiscoAIR_202601_main.parquet",)
    assert result.outputs.debug_dir == output_root / "debug"


def test_run_rollup_raises_before_running_when_validation_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    inputs = _validation_inputs(valid=False)
    _patch_validation_helpers(monkeypatch, inputs)
    monkeypatch.setattr(api, "run", lambda *args, **kwargs: pytest.fail("should not run"))

    with pytest.raises(api.RollupValidationError):
        api.run_rollup(tmp_path / "data", tmp_path / "output")


def test_run_rollup_log_file_writes_and_removes_handler(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "output"
    log_file = tmp_path / "logs" / "run.log"
    inputs = _validation_inputs()
    _patch_validation_helpers(monkeypatch, inputs)

    def fake_run(*args, **kwargs) -> None:
        output_root.mkdir(parents=True)
        (output_root / "marts").mkdir()
        (output_root / "mts_tbl_ylt_combined_all_factors.parquet").write_bytes(b"")
        (output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet").write_bytes(b"")
        (output_root / "mts_tbl_ylt_dialsup.parquet").write_bytes(b"")
        (output_root / "mts_event_validation.parquet").write_bytes(b"")
        logging.getLogger("rollup.test").info("api log file line")

    monkeypatch.setattr(api, "run", fake_run)
    monkeypatch.setattr(api, "write_ep_report", lambda output_root_arg: None)
    before_handlers = list(logging.getLogger().handlers)

    api.run_rollup(data_root, output_root, log_file=log_file)

    assert log_file.is_file()
    log_text = log_file.read_text(encoding="utf-8")
    assert "start rollup data_root=" in log_text
    assert "validation summary invalid_files=0 coverage_errors=0 is_valid=true" in log_text
    assert "api log file line" in log_text
    assert "done rollup output_root=" in log_text
    assert list(logging.getLogger().handlers) == before_handlers


def test_run_rollup_log_file_handler_removed_on_validation_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    inputs = _validation_inputs(valid=False)
    _patch_validation_helpers(monkeypatch, inputs)
    log_file = tmp_path / "run.log"
    before_handlers = list(logging.getLogger().handlers)

    with pytest.raises(api.RollupValidationError):
        api.run_rollup(tmp_path / "data", tmp_path / "output", log_file=log_file)

    assert log_file.is_file()
    assert "failed rollup elapsed=" in log_file.read_text(encoding="utf-8")
    assert list(logging.getLogger().handlers) == before_handlers


def test_generate_ep_summary_delegates_without_prompts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    expected_path = tmp_path / "data" / "ep_summaries" / "verisk" / "verisk_ep_summary.long.csv"

    def fake_generate_vendor_ep_summary(
        data_root,
        vendor,
        csv_path,
        *,
        status_callback=None,
    ) -> Path:
        assert data_root == tmp_path / "data"
        assert vendor == "verisk"
        assert csv_path == tmp_path / "source.csv"
        assert status_callback is None
        return expected_path

    monkeypatch.setattr(api, "generate_vendor_ep_summary", fake_generate_vendor_ep_summary)

    assert api.generate_ep_summary(
        tmp_path / "data",
        "verisk",
        tmp_path / "source.csv",
    ) == expected_path
