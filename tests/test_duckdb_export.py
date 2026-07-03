from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

from rollup.config import OutputConfig, RollupConfig
from rollup.duckdb_export import export_duckdb


def test_duckdb_export_reads_rollback_pipeline_output_layout(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "output"
    (output_root / "marts").mkdir(parents=True)
    write_input_files(data_root)
    pl.DataFrame({"event_id": [1, 2], "loss": [10.0, 20.0], "output_use": ["intermediate_audit", "cds_main"]}).write_parquet(
        output_root / "mts_tbl_ylt_combined_all_factors.parquet"
    )
    pl.DataFrame({"event_id": [1], "loss": [10.0], "output_use": ["cds_dialsup"]}).write_parquet(
        output_root / "mts_tbl_ylt_dialsup.parquet"
    )
    pl.DataFrame({"event_id": [1], "wide_loss": [10.0], "output_use": ["cds_wide_analysis"]}).write_parquet(
        output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet"
    )
    pl.DataFrame({"ModelEventID": [1], "ModelGrossLoss": [10.0], "source_row": ["air"]}).write_parquet(
        output_root / "marts" / "HiscoAIR_202601_euws_override.parquet"
    )
    pl.DataFrame({"ModelEventID": [2], "ModelGrossLoss": [20.0], "source_row": ["rms"]}).write_parquet(
        output_root / "marts" / "HiscoRMS_202602_dialsup_gbp_forecast.parquet"
    )

    db_path = export_duckdb(
        data_root,
        output_root,
        RollupConfig(outputs=OutputConfig(write_duckdb=True, duckdb_file="custom.duckdb")),
    )

    with duckdb.connect(str(db_path)) as connection:
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        assert {
            "cds_fanouts",
            "input_ep_summaries",
            "input_ylt_risklink",
            "input_ylt_verisk",
            "mts_tbl_ylt_combined_all_factors",
            "mts_tbl_ylt_combined_all_factors_wide",
            "mts_tbl_ylt_dialsup",
            "seed_blending_factors",
            "seed_euws_rank_overrides",
            "seed_euws_rate_factors",
            "seed_forecast_factors",
            "seed_fx_rates",
            "seed_lobs",
            "seed_perils",
        } <= tables
        assert row_count(connection, "mts_tbl_ylt_combined_all_factors") == 2
        assert row_count(connection, "mts_tbl_ylt_dialsup") == 1
        assert duckdb_columns(connection, "mts_tbl_ylt_combined_all_factors") >= {"output_use"}
        assert duckdb_columns(connection, "mts_tbl_ylt_combined_all_factors_wide") >= {"output_use"}
        assert duckdb_columns(connection, "mts_tbl_ylt_dialsup") >= {"output_use"}
        assert connection.execute(
            "SELECT output_use FROM mts_tbl_ylt_combined_all_factors ORDER BY event_id"
        ).fetchall() == [("intermediate_audit",), ("cds_main",)]
        assert connection.execute("SELECT output_use FROM mts_tbl_ylt_dialsup").fetchone() == ("cds_dialsup",)
        assert connection.execute("SELECT output_use FROM mts_tbl_ylt_combined_all_factors_wide").fetchone() == (
            "cds_wide_analysis",
        )
        assert row_count(connection, "cds_fanouts") == 2
        cds_fanout_columns = duckdb_columns(connection, "cds_fanouts")
        assert {
            "fanout_source_file",
            "fanout_name",
            "forecast_yyyymm",
            "fanout_metric",
        } <= cds_fanout_columns
        fanout_rows = connection.execute(
            """
            SELECT
                fanout_source_file,
                fanout_name,
                forecast_yyyymm,
                fanout_metric,
                ModelEventID,
                ModelGrossLoss,
                source_row
            FROM cds_fanouts
            ORDER BY fanout_source_file
            """
        ).fetchall()
        assert fanout_rows == [
            ("HiscoAIR_202601_euws_override.parquet", "HiscoAIR", "202601", "euws_override", 1, 10.0, "air"),
            (
                "HiscoRMS_202602_dialsup_gbp_forecast.parquet",
                "HiscoRMS",
                "202602",
                "dialsup_gbp_forecast",
                2,
                20.0,
                "rms",
            ),
        ]


def row_count(connection: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return connection.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]


def duckdb_columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return {row[1] for row in connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()}


def write_input_files(data_root: Path) -> None:
    (data_root / "ylt" / "verisk").mkdir(parents=True)
    (data_root / "ylt" / "risklink").mkdir(parents=True)
    (data_root / "ep_summaries" / "vendor").mkdir(parents=True)
    seeds = data_root / "seeds"
    adjustments = seeds / "adjustments"
    adjustments.mkdir(parents=True)

    pl.DataFrame({"Analysis": ["EQ"], "EventID": [1], "GroundUpLoss": [10.0]}).write_parquet(
        data_root / "ylt" / "verisk" / "verisk.parquet"
    )
    pl.DataFrame({"anlsid": [9001], "eventid": [1], "loss": [40.0]}).write_parquet(
        data_root / "ylt" / "risklink" / "risklink.parquet"
    )
    pl.DataFrame({"vendor": ["verisk", "risklink"], "loss": [1.0, 2.0]}).write_csv(
        data_root / "ep_summaries" / "vendor" / "summaries.long.csv"
    )
    pl.DataFrame({"modelled_lob": ["Fine Art"], "rollup_lob": ["Fine Art"]}).write_csv(seeds / "lobs.csv")
    pl.DataFrame({"modelled_peril": ["EQ"], "rollup_peril": ["Earthquake"]}).write_csv(seeds / "perils.csv")
    pl.DataFrame({"RegionPerilID": [205], "AIRBlend": [1.0], "RMSBlend": [0.5]}).write_csv(
        seeds / "blending_weights.csv"
    )
    pl.DataFrame({"currency_code": ["GBP"], "rate": [1.0]}).write_csv(seeds / "fx_rates.csv")
    pl.DataFrame({"forecast_date": ["2026-01-01"], "factor": [1.0]}).write_csv(seeds / "forecast_factors.csv")
    pl.DataFrame({"model_event_id": [101], "factor": [1.0]}).write_csv(seeds / "euws_rate_factors.csv")
    pl.DataFrame({"rollup_lob": ["Fine Art"], "factor": [1.0]}).write_csv(adjustments / "euws_rank_overrides.csv")
