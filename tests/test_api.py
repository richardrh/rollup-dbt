from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
import polars as pl

from rollup import api
from rollup import analysis
from rollup.analysis import build_ep_report
from rollup.config import AnalysisConfig, OutputConfig, RollupConfig
from rollup.output_contract import (
    COMBINED_YLT_FILE,
    DIALSUP_YLT_FILE,
)
from rollup.writers import duckdb_export


def test_run_rollup_returns_dataiku_friendly_output_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "output"
    db_path = output_root / "rollup.duckdb"
    events: list[str] = []

    def fake_run(
        data_root_arg: Path, *, output_root: Path, debug: bool, config: RollupConfig
    ) -> object:
        assert data_root_arg == data_root
        assert debug is True
        assert config.outputs.minimum_event_loss_threshold == 1000.0
        events.append("run")
        (output_root / "marts").mkdir(parents=True)
        (output_root / "marts" / "HiscoAIR_202601_main.parquet").write_bytes(b"")
        (output_root / "mts_tbl_ylt_combined_all_factors.parquet").write_bytes(b"")
        (output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet").write_bytes(b"")
        (output_root / "mts_tbl_ylt_dialsup.parquet").write_bytes(b"")
        (output_root / "mts_event_validation.parquet").write_bytes(b"")
        return object()

    def fake_write_ep_report(output_root_arg: Path, **kwargs) -> Path:
        events.append("write_ep_report")
        path = output_root_arg / "analysis" / "ep_report.csv"
        path.parent.mkdir(parents=True)
        path.write_text("ok\n", encoding="utf-8")
        return path

    monkeypatch.setattr(api, "run_pipeline", fake_run)
    monkeypatch.setattr(api, "write_ep_report", fake_write_ep_report)
    monkeypatch.setattr(
        duckdb_export,
        "write",
        lambda data_root_arg, output_root_arg, config: (
            events.append("export_duckdb") or db_path
        ),
    )

    result = api.run_rollup(data_root, output_root, debug=True)

    assert events == ["run", "write_ep_report", "export_duckdb"]
    assert result.data_root == data_root
    assert result.output_root == output_root
    assert (
        result.mts_combined == output_root / "mts_tbl_ylt_combined_all_factors.parquet"
    )
    assert (
        result.mts_wide == output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet"
    )
    assert result.mts_dialsup == output_root / "mts_tbl_ylt_dialsup.parquet"
    assert result.mart_files == (
        output_root / "marts" / "HiscoAIR_202601_main.parquet",
    )
    assert result.debug_dir == output_root / "debug"
    assert result.duckdb_file == db_path


def test_run_rollup_log_file_writes_jsonl(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output_root = tmp_path / "output"
    log_file = tmp_path / "logs" / "run.jsonl"

    def fake_run(*args, **kwargs) -> None:
        output_root.mkdir(parents=True)
        (output_root / "marts").mkdir()
        (output_root / "mts_tbl_ylt_combined_all_factors.parquet").write_bytes(b"")
        (output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet").write_bytes(b"")
        (output_root / "mts_tbl_ylt_dialsup.parquet").write_bytes(b"")
        (output_root / "mts_event_validation.parquet").write_bytes(b"")
        logging.getLogger("rollup.test").info("api log file line")

    monkeypatch.setattr(api, "run_pipeline", fake_run)
    monkeypatch.setattr(api, "write_ep_report", lambda output_root_arg, **kwargs: None)

    api.run_rollup(
        tmp_path / "data",
        output_root,
        config=RollupConfig(outputs=OutputConfig(write_duckdb=False)),
        log_file=log_file,
        log_format="jsonl",
    )

    rows = [
        json.loads(line) for line in log_file.read_text(encoding="utf-8").splitlines()
    ]
    assert any(row["message"] == "api log file line" for row in rows)
    assert all(
        "timestamp" in row and row["logger"].startswith("rollup") for row in rows
    )


def test_run_rollup_writes_duckdb_when_configured(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output_root = tmp_path / "output"
    db_path = output_root / "custom.duckdb"

    def fake_run(*args, **kwargs) -> None:
        output_root.mkdir(parents=True)

    monkeypatch.setattr(api, "run_pipeline", fake_run)
    monkeypatch.setattr(api, "write_ep_report", lambda output_root_arg, **kwargs: None)
    monkeypatch.setattr(
        duckdb_export, "write", lambda data_root, output_root_arg, config: db_path
    )

    result = api.run_rollup(
        tmp_path / "data",
        output_root,
        config=RollupConfig(
            outputs=OutputConfig(write_duckdb=True, duckdb_file="custom.duckdb")
        ),
    )

    assert result.duckdb_file == db_path


def test_run_rollup_skips_duckdb_when_explicitly_disabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output_root = tmp_path / "output"
    exported = False

    def fake_run(*args, **kwargs) -> None:
        output_root.mkdir(parents=True)

    def fake_export_duckdb(*args, **kwargs) -> Path:
        nonlocal exported
        exported = True
        return output_root / "rollup.duckdb"

    monkeypatch.setattr(api, "run_pipeline", fake_run)
    monkeypatch.setattr(api, "write_ep_report", lambda output_root_arg, **kwargs: None)
    monkeypatch.setattr(duckdb_export, "write", fake_export_duckdb)

    result = api.run_rollup(
        tmp_path / "data",
        output_root,
        config=RollupConfig(outputs=OutputConfig(write_duckdb=False)),
    )

    assert exported is False
    assert result.duckdb_file is None


def test_run_rollup_passes_config_to_pipeline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output_root = tmp_path / "output"
    configured = RollupConfig(
        outputs=OutputConfig(write_duckdb=False, minimum_event_loss_threshold=999_999.0)
    )

    def fake_run(*args, **kwargs) -> None:
        assert kwargs["config"] is configured
        output_root.mkdir(parents=True)

    monkeypatch.setattr(api, "run_pipeline", fake_run)
    monkeypatch.setattr(api, "write_ep_report", lambda output_root_arg, **kwargs: None)

    api.run_rollup(tmp_path / "data", output_root, config=configured)


def test_analysis_report_uses_supplied_config_without_module_reload(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "output"
    main = pl.DataFrame(
        {
            "forecast_date": ["2026-01-01", "2026-01-01"],
            "base_model": ["verisk", "verisk"],
            "rollup_lob": ["LOB", "LOB"],
            "rollup_peril": ["PERIL", "PERIL"],
            "year_id": [1, 2],
            "metric": ["euws_override", "euws_override"],
            "loss": [100.0, 50.0],
        }
    )
    dialsup = pl.DataFrame(schema=main.schema)
    output_root.mkdir(parents=True)
    main.write_parquet(output_root / COMBINED_YLT_FILE)
    dialsup.write_parquet(output_root / DIALSUP_YLT_FILE)

    report_two = build_ep_report(
        output_root,
        config=RollupConfig(
            analysis=AnalysisConfig(
                simulation_counts={"verisk": 2}, return_periods=(2,)
            )
        ),
    )
    report_four = build_ep_report(
        output_root,
        config=RollupConfig(
            analysis=AnalysisConfig(
                simulation_counts={"verisk": 4}, return_periods=(4,)
            )
        ),
    )

    assert set(report_two.get_column("return_period")) == {0, 2}
    assert set(report_four.get_column("return_period")) == {0, 4}
    assert report_two.filter(pl.col("ep_type") == "AAL").item(0, "loss") == 75.0
    assert report_four.filter(pl.col("ep_type") == "AAL").item(0, "loss") == 37.5


def test_analysis_report_write_preserves_existing_output_when_csv_write_fails(
    tmp_path: Path, monkeypatch
) -> None:
    output_path = tmp_path / "analysis" / "ep_report.csv"
    output_path.parent.mkdir()
    output_path.write_text("sentinel\nkeep\n", encoding="utf-8")
    monkeypatch.setattr(
        analysis,
        "build_ep_report",
        lambda *args, **kwargs: pl.DataFrame({"value": [1]}),
    )

    def fail_write_csv(self, path, *args, **kwargs) -> None:
        raise OSError("write failed")

    monkeypatch.setattr(pl.DataFrame, "write_csv", fail_write_csv)

    with pytest.raises(OSError, match="write failed"):
        analysis.write_ep_report(tmp_path)

    assert output_path.read_text(encoding="utf-8") == "sentinel\nkeep\n"
