from __future__ import annotations

import logging
import time
import tempfile
from collections import Counter
from pathlib import Path
from typing import Literal

import duckdb

from rollup.config import RollupConfig
from rollup.output_contract import ANALYSIS_DIR, EP_REPORT_FILE, MARTS_DIR
from rollup.writers._sql import identifier as qid
from rollup.writers._sql import literal as qlit

logger = logging.getLogger(__name__)
Source = tuple[str, Path, Literal["parquet", "csv"]]


def validate(
    data_root: str | Path, output_root: str | Path, config: RollupConfig
) -> tuple[Path, Path, list[Source]]:
    if not isinstance(config, RollupConfig):
        raise TypeError("duckdb_export: config must be a RollupConfig")
    try:
        data_root = Path(data_root)
        output_root = Path(output_root)
    except TypeError as exc:
        raise TypeError(
            "duckdb_export: data_root and output_root must be path-like"
        ) from exc
    sources = _source_inventory(data_root, output_root)
    if not sources:
        raise ValueError("duckdb_export: no parquet or csv sources found to export")
    duplicates = sorted(
        table_name
        for table_name, count in Counter(source[0] for source in sources).items()
        if count > 1
    )
    if duplicates:
        raise ValueError(f"duckdb_export: duplicate table names: {duplicates}")
    missing_paths = [str(path) for _, path, _ in sources if not path.exists()]
    if missing_paths:
        raise FileNotFoundError(
            f"duckdb_export: selected source paths do not exist: {missing_paths}"
        )
    return data_root, output_root, sources


def write(data_root: str | Path, output_root: str | Path, config: RollupConfig) -> Path:
    _, output_root, sources = validate(data_root, output_root, config)
    db_path = config.outputs.duckdb_path(output_root)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    logger.info(
        "writing duckdb export path=%s",
        db_path,
        extra={"event": "duckdb_export_start", "path": db_path},
    )
    with tempfile.NamedTemporaryFile(
        suffix=".duckdb", prefix=f".{db_path.stem}-", dir=db_path.parent, delete=False
    ) as handle:
        staged_path = Path(handle.name)
    staged_path.unlink()
    try:
        connection = duckdb.connect(str(staged_path))
        try:
            for table_name, source_path, source_format in sources:
                _create_table(connection, table_name, source_path, source_format)
        finally:
            connection.close()
        staged_path.replace(db_path)
    finally:
        staged_path.unlink(missing_ok=True)
    elapsed_seconds = time.perf_counter() - started
    logger.info(
        "wrote duckdb export path=%s elapsed=%.2fs",
        db_path,
        elapsed_seconds,
        extra={
            "event": "duckdb_export_done",
            "path": db_path,
            "elapsed_seconds": elapsed_seconds,
        },
    )
    return db_path


def _source_inventory(data_root: Path, output_root: Path) -> list[Source]:
    sources: list[Source] = []
    for path in sorted(output_root.glob("mts_tbl_*.parquet")):
        sources.append((path.stem, path, "parquet"))
    for path in sorted((output_root / MARTS_DIR).glob("*.parquet")):
        sources.append((path.stem, path, "parquet"))
    for path, source_format in _seed_file_paths(data_root):
        sources.append((path.stem, path, source_format))
    analysis_report_path = output_root / ANALYSIS_DIR / EP_REPORT_FILE
    if analysis_report_path.exists():
        sources.append(("ep_report", analysis_report_path, "csv"))
    return sources


def _seed_file_paths(data_root: Path) -> list[tuple[Path, Literal["parquet", "csv"]]]:
    seeds_root = data_root / "seeds"
    if not seeds_root.exists():
        return []
    paths: list[tuple[Path, Literal["parquet", "csv"]]] = []
    paths.extend((path, "csv") for path in sorted(seeds_root.rglob("*.csv")))
    paths.extend((path, "parquet") for path in sorted(seeds_root.rglob("*.parquet")))
    return paths


def _create_table(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    path: Path,
    source_format: Literal["parquet", "csv"],
) -> None:
    reader = "read_parquet" if source_format == "parquet" else "read_csv_auto"
    filename = ", filename = true" if source_format == "csv" else ""
    connection.execute(
        f"CREATE TABLE {qid(table_name)} AS "
        f"SELECT * FROM {reader}({qlit(path.expanduser().resolve(strict=False))}{filename}, union_by_name = true)"
    )
