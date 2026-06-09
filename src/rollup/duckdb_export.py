from __future__ import annotations

from pathlib import Path
import logging

import duckdb

from rollup.config import RollupConfig

logger = logging.getLogger(__name__)


def export_duckdb(data_root: str | Path, output_root: str | Path, config: RollupConfig) -> Path:
    data_root = Path(data_root)
    output_root = Path(output_root)
    db_path = config.outputs.duckdb_path(output_root)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    logger.info("writing duckdb export path=%s", db_path)
    connection = duckdb.connect(str(db_path))
    try:
        marts_dir = config.outputs.marts_path(output_root)
        create_parquet_table(
            connection,
            "mts_tbl_ylt_combined_all_factors",
            [marts_dir / config.outputs.combined_file],
        )
        create_parquet_table(connection, "input_ylt_verisk", sorted((data_root / "ylt" / "verisk").glob("*.parquet")))
        create_parquet_table(
            connection,
            "input_ylt_risklink",
            sorted((data_root / "ylt" / "risklink").glob("*.parquet")),
        )
        create_csv_table(
            connection,
            "input_ep_summaries",
            sorted((data_root / "ep_summaries").rglob("*.long.csv")),
        )

        create_optional_csv_table(connection, "seed_lobs", discover_seed_file(data_root, ("lobs.csv",)))
        create_optional_csv_table(connection, "seed_perils", discover_seed_file(data_root, ("perils.csv",)))
        create_optional_csv_table(
            connection,
            "seed_blending_factors",
            discover_seed_file(data_root, ("blending_factors.csv", "blending_weights.csv")),
        )
        create_optional_csv_table(connection, "seed_fx_rates", discover_seed_file(data_root, ("fx_rates.csv",)))
        create_optional_csv_table(
            connection,
            "seed_forecast_factors",
            discover_seed_file(data_root, ("forecast_factors.csv",)),
        )
        create_optional_csv_table(
            connection,
            "seed_euws_rate_factors",
            discover_seed_file(data_root, ("euws_rate_factors.csv",)),
        )
        create_optional_csv_table(
            connection,
            "seed_euws_rank_overrides",
            data_root / "seeds" / "adjustments" / "euws_rank_overrides.csv",
        )
        create_optional_parquet_table(
            connection,
            "seed_verisk_events",
            data_root / "seeds" / "validation" / "verisk_events.parquet",
        )
        create_optional_parquet_table(
            connection,
            "seed_risklink_flood22_model_events",
            data_root / "seeds" / "validation" / "risklink_flood22_model_events.parquet",
        )
    finally:
        connection.close()
    return db_path


def create_parquet_table(connection: duckdb.DuckDBPyConnection, table_name: str, paths: list[Path]) -> None:
    if not paths:
        raise FileNotFoundError(f"no parquet files found for DuckDB table {table_name}")
    connection.execute(
        f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet({path_list(paths)}, union_by_name = true)"
    )


def create_optional_parquet_table(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    path: Path,
) -> None:
    if path.exists():
        create_parquet_table(connection, table_name, [path])


def create_csv_table(connection: duckdb.DuckDBPyConnection, table_name: str, paths: list[Path]) -> None:
    if not paths:
        raise FileNotFoundError(f"no CSV files found for DuckDB table {table_name}")
    connection.execute(
        f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto({path_list(paths)}, union_by_name = true)"
    )


def create_optional_csv_table(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    path: Path | None,
) -> None:
    if path is not None and path.exists():
        create_csv_table(connection, table_name, [path])


def discover_seed_file(data_root: Path, filenames: tuple[str, ...]) -> Path | None:
    for filename in filenames:
        paths = sorted((data_root / "seeds").rglob(filename), key=lambda path: (len(path.parts), str(path)))
        if paths:
            return paths[0]
    return None


def path_list(paths: list[Path]) -> str:
    return "[" + ", ".join(sql_string(path) for path in paths) + "]"


def sql_string(path: Path) -> str:
    return "'" + str(path.expanduser().resolve(strict=False)).replace("'", "''") + "'"
