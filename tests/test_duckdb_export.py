from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl
import pytest

from rollup.config import OutputConfig, RollupConfig
from rollup.writers import duckdb_export


def test_duckdb_export_reads_current_pipeline_output_layout(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "output"
    (output_root / "marts").mkdir(parents=True)
    write_input_files(data_root)
    pl.DataFrame(
        {
            "event_id": [1, 2],
            "loss": [10.0, 20.0],
            "output_use": ["intermediate_audit", "cds_main"],
        }
    ).write_parquet(output_root / "mts_tbl_ylt_combined_all_factors.parquet")
    pl.DataFrame(
        {"event_id": [1], "loss": [10.0], "output_use": ["cds_dialsup"]}
    ).write_parquet(output_root / "mts_tbl_ylt_dialsup.parquet")
    pl.DataFrame(
        {"event_id": [1], "wide_loss": [10.0], "output_use": ["cds_wide_analysis"]}
    ).write_parquet(output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet")
    (output_root / "analysis").mkdir()
    pl.DataFrame({"metric": ["main"], "loss": [10.0]}).write_csv(
        output_root / "analysis" / "ep_report.csv"
    )
    pl.DataFrame(
        {"ModelEventID": [1], "ModelGrossLoss": [10.0], "source_row": ["air"]}
    ).write_parquet(output_root / "marts" / "HiscoAIR_202601_euws_override.parquet")
    pl.DataFrame(
        {"ModelEventID": [2], "ModelGrossLoss": [20.0], "source_row": ["rms"]}
    ).write_parquet(
        output_root / "marts" / "HiscoRMS_202602_dialsup_localccy_forecast.parquet"
    )
    pl.DataFrame(
        {"event_id": [3], "loss": [30.0], "output_use": ["nested_mts"]}
    ).write_parquet(output_root / "marts" / "mts_tbl_nested.parquet")

    db_path = duckdb_export.write(
        data_root,
        output_root,
        RollupConfig(
            outputs=OutputConfig(write_duckdb=True, duckdb_file="custom.duckdb")
        ),
    )

    with duckdb.connect(str(db_path)) as connection:
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        assert {
            "ep_report",
            "HiscoAIR_202601_euws_override",
            "HiscoRMS_202602_dialsup_localccy_forecast",
            "mts_tbl_ylt_combined_all_factors",
            "mts_tbl_ylt_combined_all_factors_wide",
            "mts_tbl_ylt_dialsup",
            "mts_tbl_nested",
            "blending_factors",
            "euws_rank_overrides",
            "euws_rate_factors",
            "event_validation",
            "forecast_factors",
            "fx_rates",
            "lobs",
            "perils",
        } <= tables
        assert row_count(connection, "mts_tbl_ylt_combined_all_factors") == 2
        assert row_count(connection, "mts_tbl_ylt_dialsup") == 1
        assert row_count(connection, "mts_tbl_nested") == 1
        assert duckdb_columns(connection, "mts_tbl_ylt_combined_all_factors") >= {
            "output_use"
        }
        assert duckdb_columns(connection, "mts_tbl_ylt_combined_all_factors_wide") >= {
            "output_use"
        }
        assert duckdb_columns(connection, "mts_tbl_ylt_dialsup") >= {"output_use"}
        assert duckdb_columns(connection, "mts_tbl_nested") >= {"output_use"}
        assert connection.execute(
            "SELECT output_use FROM mts_tbl_ylt_combined_all_factors ORDER BY event_id"
        ).fetchall() == [("intermediate_audit",), ("cds_main",)]
        assert connection.execute(
            "SELECT output_use FROM mts_tbl_ylt_dialsup"
        ).fetchone() == ("cds_dialsup",)
        assert connection.execute(
            "SELECT output_use FROM mts_tbl_nested"
        ).fetchone() == ("nested_mts",)
        assert connection.execute(
            "SELECT output_use FROM mts_tbl_ylt_combined_all_factors_wide"
        ).fetchone() == ("cds_wide_analysis",)
        assert connection.execute("SELECT metric, loss FROM ep_report").fetchone() == (
            "main",
            10.0,
        )
        assert row_count(connection, "lobs") == 1
        assert row_count(connection, "perils") == 1
        assert row_count(connection, "fx_rates") == 1
        assert row_count(connection, "HiscoAIR_202601_euws_override") == 1
        assert "seeds" not in tables
        assert "cds_fanouts" not in tables
        assert "input_ylt_verisk" not in tables


def test_duckdb_export_skips_missing_ep_report_and_quotes_paths(tmp_path: Path) -> None:
    data_root = tmp_path / "data's root"
    output_root = tmp_path / "output's root"
    seeds = data_root / "seeds"
    seeds.mkdir(parents=True)
    output_root.mkdir(parents=True)
    parquet_table = "mts_tbl_owner's_losses"
    pl.DataFrame({"event_id": [1], "loss": [10.0]}).write_parquet(
        output_root / f"{parquet_table}.parquet"
    )
    pl.DataFrame({"seed_id": [1], "seed_value": ["ok"]}).write_csv(seeds / "seed's.csv")

    db_path = duckdb_export.write(data_root, output_root, RollupConfig())

    with duckdb.connect(str(db_path)) as connection:
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        assert parquet_table in tables
        assert "seed's" in tables
        assert "ep_report" not in tables
        assert row_count(connection, parquet_table) == 1
        assert row_count(connection, "seed's") == 1


def test_duckdb_export_preserves_existing_db_when_validation_rejects_duplicates(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "output"
    marts = output_root / "marts"
    marts.mkdir(parents=True)
    pl.DataFrame({"value": [1]}).write_parquet(
        output_root / "mts_tbl_duplicate.parquet"
    )
    pl.DataFrame({"value": [2]}).write_parquet(marts / "mts_tbl_duplicate.parquet")
    db_path = RollupConfig().outputs.duckdb_path(output_root)
    db_path.write_bytes(b"preserve me")

    try:
        duckdb_export.write(data_root, output_root, RollupConfig())
    except ValueError as exc:
        assert "duplicate table names" in str(exc)
    else:
        raise AssertionError("expected duplicate table name validation failure")

    assert db_path.read_bytes() == b"preserve me"


def test_duckdb_export_preserves_existing_db_when_table_creation_fails(
    tmp_path: Path, monkeypatch
) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "output"
    output_root.mkdir()
    pl.DataFrame({"value": [1]}).write_parquet(output_root / "mts_tbl_output.parquet")
    db_path = RollupConfig().outputs.duckdb_path(output_root)
    db_path.write_bytes(b"preserve me")

    def fail_create(*args, **kwargs) -> None:
        raise RuntimeError("table creation failed")

    monkeypatch.setattr(duckdb_export, "_create_table", fail_create)

    with pytest.raises(RuntimeError, match="table creation failed"):
        duckdb_export.write(data_root, output_root, RollupConfig())

    assert db_path.read_bytes() == b"preserve me"


def row_count(connection: duckdb.DuckDBPyConnection, table_name: str) -> int:
    quoted_table_name = '"' + table_name.replace('"', '""') + '"'
    row = connection.execute(f"SELECT count(*) FROM {quoted_table_name}").fetchone()
    assert row is not None
    return int(row[0])


def duckdb_columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return {
        row[1]
        for row in connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    }


def write_input_files(data_root: Path) -> None:
    (data_root / "ylt" / "verisk").mkdir(parents=True)
    (data_root / "ylt" / "risklink").mkdir(parents=True)
    (data_root / "ep_summaries" / "vendor").mkdir(parents=True)
    seeds = data_root / "seeds"
    adjustments = seeds / "adjustments"
    adjustments.mkdir(parents=True)
    validation = seeds / "validation"
    validation.mkdir(parents=True)

    pl.DataFrame(
        {"Analysis": ["EQ"], "EventID": [1], "GroundUpLoss": [10.0]}
    ).write_parquet(data_root / "ylt" / "verisk" / "verisk.parquet")
    pl.DataFrame({"anlsid": [9001], "eventid": [1], "loss": [40.0]}).write_parquet(
        data_root / "ylt" / "risklink" / "risklink.parquet"
    )
    pl.DataFrame({"vendor": ["verisk", "risklink"], "loss": [1.0, 2.0]}).write_csv(
        data_root / "ep_summaries" / "vendor" / "summaries.long.csv"
    )
    pl.DataFrame({"modelled_lob": ["Fine Art"], "rollup_lob": ["Fine Art"]}).write_csv(
        seeds / "lobs.csv"
    )
    pl.DataFrame({"modelled_peril": ["EQ"], "rollup_peril": ["Earthquake"]}).write_csv(
        seeds / "perils.csv"
    )
    pl.DataFrame(
        {"RegionPerilID": [205], "AIRBlend": [1.0], "RMSBlend": [0.5]}
    ).write_csv(seeds / "blending_factors.csv")
    pl.DataFrame({"currency_code": ["GBP"], "rate": [1.0]}).write_csv(
        seeds / "fx_rates.csv"
    )
    pl.DataFrame({"forecast_date": ["2026-01-01"], "factor": [1.0]}).write_csv(
        seeds / "forecast_factors.csv"
    )
    pl.DataFrame({"model_event_id": [101], "factor": [1.0]}).write_csv(
        seeds / "euws_rate_factors.csv"
    )
    pl.DataFrame({"rollup_lob": ["Fine Art"], "factor": [1.0]}).write_csv(
        adjustments / "euws_rank_overrides.csv"
    )
    pl.DataFrame({"validation_id": [1]}).write_csv(validation / "event_validation.csv")
