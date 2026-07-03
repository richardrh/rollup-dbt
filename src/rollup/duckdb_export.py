from __future__ import annotations

from pathlib import Path
import logging
import time

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

    started = time.perf_counter()
    logger.info("writing duckdb export path=%s", db_path, extra={"event": "duckdb_export_start", "path": db_path})
    connection = duckdb.connect(str(db_path))
    try:
        for path in sorted(output_root.glob("mts_tbl_*.parquet")):
            create_parquet_table(connection, path.stem, [path])
        create_optional_csv_glob_table(connection, "ep_report", output_root / config.outputs.analysis_dir / config.outputs.ep_report_file)
        create_csv_glob_table(connection, "seeds", data_root / "seeds" / "**" / "*.csv")
    finally:
        connection.close()
    elapsed_seconds = time.perf_counter() - started
    logger.info(
        "wrote duckdb export path=%s elapsed=%.2fs",
        db_path,
        elapsed_seconds,
        extra={"event": "duckdb_export_done", "path": db_path, "elapsed_seconds": elapsed_seconds},
    )
    return db_path


def create_parquet_table(connection: duckdb.DuckDBPyConnection, table_name: str, paths: list[Path]) -> None:
    if not paths:
        raise FileNotFoundError(f"no parquet files found for DuckDB table {table_name}")
    logger.info(
        "creating duckdb parquet table table=%s files=%d",
        table_name,
        len(paths),
        extra={"event": "duckdb_create_table", "table": table_name, "files": len(paths), "source_format": "parquet"},
    )
    connection.execute(
        f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet({path_list(paths)}, union_by_name = true)"
    )


def create_csv_glob_table(connection: duckdb.DuckDBPyConnection, table_name: str, path: Path) -> None:
    glob_path = str(path.expanduser().resolve(strict=False))
    logger.info(
        "creating duckdb csv table table=%s glob=%s",
        table_name,
        glob_path,
        extra={"event": "duckdb_create_table", "table": table_name, "glob": glob_path, "source_format": "csv"},
    )
    connection.execute(
        f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto({sql_string(path)}, filename = true, union_by_name = true)"
    )


def create_optional_csv_glob_table(connection: duckdb.DuckDBPyConnection, table_name: str, path: Path) -> None:
    if path.exists():
        create_csv_glob_table(connection, table_name, path)


def path_list(paths: list[Path]) -> str:
    return "[" + ", ".join(sql_string(path) for path in paths) + "]"


def sql_string(path: Path) -> str:
    return "'" + str(path.expanduser().resolve(strict=False)).replace("'", "''") + "'"
