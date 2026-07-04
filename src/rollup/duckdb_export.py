from __future__ import annotations

from pathlib import Path
import logging
import time
from typing import Literal

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
        sources: list[tuple[str, Path, Literal["parquet", "csv"]]] = [
            (path.stem, path, "parquet") for path in sorted(output_root.glob("mts_tbl_*.parquet"))
        ]
        ep_report_path = output_root / config.outputs.analysis_dir / config.outputs.ep_report_file
        if ep_report_path.exists():
            sources.append(("ep_report", ep_report_path, "csv"))
        sources.append(("seeds", data_root / "seeds" / "**" / "*.csv", "csv"))

        for table_name, source_path, source_format in sources:
            create_table(connection, table_name, source_path, source_format)
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


def create_table(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    path: Path,
    source_format: Literal["parquet", "csv"],
) -> None:
    source_path = str(path.expanduser().resolve(strict=False))
    quoted_table_name = '"' + table_name.replace('"', '""') + '"'
    quoted_source_path = "'" + source_path.replace("'", "''") + "'"
    reader = "read_parquet" if source_format == "parquet" else "read_csv_auto"
    filename = ", filename = true" if source_format == "csv" else ""
    connection.execute(
        f"CREATE OR REPLACE TABLE {quoted_table_name} AS "
        f"SELECT * FROM {reader}({quoted_source_path}{filename}, union_by_name = true)"
    )
