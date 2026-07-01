from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

from rollup.config import OutputConfig, RollupConfig
from rollup.duckdb_export import export_duckdb


def test_duckdb_export_writes_requested_tables_without_mart_fanouts(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    output_root = tmp_path / "output"
    marts_dir = output_root / "marts"
    marts_dir.mkdir(parents=True)
    write_input_files(data_root)
    pl.DataFrame({"event_id": [1, 2], "loss": [10.0, 20.0]}).write_parquet(
        marts_dir / "mts_tbl_ylt_combined_all_factors.parquet"
    )
    pl.DataFrame({"event_id": [1], "loss": [10.0]}).write_parquet(marts_dir / "mts_tbl_ylt_dialsup.parquet")
    pl.DataFrame({"event_id": [1], "loss": [10.0]}).write_parquet(marts_dir / "HiscoAIR_20260101_main.parquet")

    db_path = export_duckdb(
        data_root,
        output_root,
        RollupConfig(outputs=OutputConfig(write_duckdb=True, duckdb_file="custom.duckdb")),
    )

    assert db_path == output_root / "custom.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        assert tables == {
            "input_ep_summaries",
            "input_ylt_risklink",
            "input_ylt_verisk",
            "mts_tbl_ylt_combined_all_factors",
            "mts_tbl_ylt_dialsup",
            "seed_blending_factors",
            "seed_euws_rank_overrides",
            "seed_euws_rate_factors",
            "seed_forecast_factors",
            "seed_fx_rates",
            "seed_lobs",
            "seed_perils",
        }
        assert row_count(connection, "mts_tbl_ylt_combined_all_factors") == 2
        assert row_count(connection, "mts_tbl_ylt_dialsup") == 1
        assert row_count(connection, "input_ylt_verisk") == 1
        assert row_count(connection, "input_ylt_risklink") == 1
        assert row_count(connection, "input_ep_summaries") == 2
        assert row_count(connection, "seed_blending_factors") == 1


def row_count(connection: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return connection.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]


def write_input_files(data_root: Path) -> None:
    (data_root / "ylt" / "verisk").mkdir(parents=True)
    (data_root / "ylt" / "risklink").mkdir(parents=True)
    (data_root / "ep_summaries" / "vendor" / "nested").mkdir(parents=True)
    seeds = data_root / "seeds"
    adjustments = seeds / "adjustments"
    validation = seeds / "validation"
    adjustments.mkdir(parents=True)
    validation.mkdir(parents=True)

    pl.DataFrame({"Analysis": ["EQ"], "EventID": [1], "GroundUpLoss": [10.0]}).write_parquet(
        data_root / "ylt" / "verisk" / "verisk.parquet"
    )
    pl.DataFrame({"anlsid": [9001], "eventid": [1], "loss": [40.0]}).write_parquet(
        data_root / "ylt" / "risklink" / "risklink.parquet"
    )
    pl.DataFrame({"vendor": ["verisk", "risklink"], "loss": [1.0, 2.0]}).write_csv(
        data_root / "ep_summaries" / "vendor" / "nested" / "summaries.long.csv"
    )
    pl.DataFrame({"modelled_lob": ["Fine Art"], "rollup_lob": ["Fine Art"]}).write_csv(seeds / "lobs.csv")
    pl.DataFrame({"modelled_peril": ["EQ"], "rollup_peril": ["Earthquake"]}).write_csv(seeds / "perils.csv")
    pl.DataFrame({"RegionPerilID": [205], "AIRBlend": [1.0], "RMSBlend": [0.5]}).write_csv(
        seeds / "blending_weights.csv"
    )
    pl.DataFrame({"currency_code": ["GBP"], "rate": [1.0]}).write_csv(seeds / "fx_rates.csv")
    pl.DataFrame({"forecast_date": ["2026-01-01"], "factor": [1.0]}).write_csv(seeds / "forecast_factors.csv")
    pl.DataFrame({"model_event_id": [101], "factor": [1.0]}).write_csv(seeds / "euws_rate_factors.csv")
    pl.DataFrame({"rollup_lob": ["Fine Art"], "factor": [1.0]}).write_csv(
        adjustments / "euws_rank_overrides.csv"
    )
    pl.DataFrame({"EventID": [101], "ModelID": [7], "Event": [1]}).write_parquet(
        validation / "verisk_events.parquet"
    )
    pl.DataFrame({"EventID": [202], "Event": [2], "Peril": ["Flood"]}).write_parquet(
        validation / "risklink_flood22_model_events.parquet"
    )
