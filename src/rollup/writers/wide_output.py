from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import polars as pl

from rollup.columns import Col
from rollup.output_contract import WIDE_DIAGNOSTIC_COLUMNS, WIDE_IDENTITY_DIMENSIONS
from rollup.writers._sql import identifier as qid
from rollup.writers._sql import literal as qlit

logger = logging.getLogger(__name__)


def validate(ylt_path: Path, dialsup_path: Path, output_path: Path) -> None:
    for name, path in {
        "ylt_path": ylt_path,
        "dialsup_path": dialsup_path,
        "output_path": output_path,
    }.items():
        if not isinstance(path, Path):
            raise TypeError(f"wide_output: {name} must be a pathlib.Path")
        if path.suffix != ".parquet":
            raise ValueError(f"wide_output: {name} must have a .parquet suffix")
    for name, path in {"ylt_path": ylt_path, "dialsup_path": dialsup_path}.items():
        if not path.exists():
            raise FileNotFoundError(f"wide_output: {name} does not exist: {path}")
    required = [
        *WIDE_IDENTITY_DIMENSIONS,
        Col.output_use,
        Col.metric,
        Col.forecast_date,
        Col.loss,
    ]
    main_required = [*required, *WIDE_DIAGNOSTIC_COLUMNS]
    for input_name, path, columns in [
        ("ylt_path", ylt_path, main_required),
        ("dialsup_path", dialsup_path, required),
    ]:
        schema = pl.scan_parquet(path).collect_schema()
        missing = [column for column in columns if column not in schema]
        if missing:
            raise ValueError(
                f"wide_output: input '{input_name}' missing required columns {missing}"
            )


def write(ylt_path: Path, dialsup_path: Path, output_path: Path) -> Path:
    validate(ylt_path, dialsup_path, output_path)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "rollup.writers.wide_output",
            str(ylt_path),
            str(dialsup_path),
            str(output_path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "wide_output: worker failed")
    logger.info("wrote wide output=%s", output_path)
    return output_path


def _write_worker(ylt_path: Path, dialsup_path: Path, output_path: Path) -> Path:
    import duckdb

    started = time.perf_counter()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="rollup-duckdb-wide-", dir=output_path.parent
    ) as temp_dir:
        temp_output_path = Path(temp_dir) / f"{output_path.stem}.tmp.parquet"
        con = duckdb.connect()
        try:
            con.execute(f"SET temp_directory={qlit(Path(temp_dir))}")
            con.execute("SET memory_limit='8GB'")
            dates = _forecast_dates(con, ylt_path, dialsup_path)
            if not dates:
                raise ValueError("wide_output: no forecast dates found")
            _raise_on_duplicate_grain(con, ylt_path, "euws_override")
            _raise_on_duplicate_grain(con, dialsup_path, "dialsup_localccy_forecast")
            query = _wide_copy_query(ylt_path, dialsup_path, temp_output_path, dates)
            con.execute(query)
            row = con.execute(
                f"SELECT COUNT(*) FROM read_parquet({qlit(temp_output_path)})"
            ).fetchone()
            row_count = int(row[0]) if row is not None else 0
        finally:
            con.close()
        temp_output_path.replace(output_path)
    elapsed_seconds = time.perf_counter() - started
    logger.info(
        "wrote output=%s rows=%d elapsed=%.2fs",
        output_path,
        row_count,
        elapsed_seconds,
        extra={
            "event": "write_output",
            "path": output_path,
            "rows": row_count,
            "elapsed_seconds": elapsed_seconds,
            "lazy": False,
        },
    )
    return output_path


def _forecast_dates(con, ylt_path: Path, dialsup_path: Path) -> list[str]:
    rows = con.execute(
        f"""
        SELECT DISTINCT CAST({qid(Col.forecast_date)} AS VARCHAR) AS forecast_date
        FROM (
            SELECT {qid(Col.forecast_date)} FROM read_parquet({qlit(ylt_path)}) WHERE {qid(Col.metric)} = 'euws_override'
            UNION ALL
            SELECT {qid(Col.forecast_date)} FROM read_parquet({qlit(dialsup_path)}) WHERE {qid(Col.metric)} = 'dialsup_localccy_forecast'
        )
        WHERE forecast_date IS NOT NULL
        ORDER BY forecast_date
        """
    ).fetchall()
    return [row[0] for row in rows]


def _raise_on_duplicate_grain(con, path: Path, metric: str) -> None:
    dims = ", ".join(qid(column) for column in WIDE_IDENTITY_DIMENSIONS)
    duplicate_count = con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT {dims}, {qid(Col.metric)}, {qid(Col.forecast_date)}, COUNT(*) AS row_count
            FROM read_parquet({qlit(path)})
            WHERE {qid(Col.metric)} = {qlit(metric)}
            GROUP BY {dims}, {qid(Col.metric)}, {qid(Col.forecast_date)}
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    if duplicate_count:
        raise ValueError(
            f"wide_output: duplicate grain rows for metric {metric}: {duplicate_count} duplicate groups"
        )


def _source_union(ylt_path: Path, dialsup_path: Path) -> str:
    columns = ", ".join(
        qid(column)
        for column in [
            *WIDE_IDENTITY_DIMENSIONS,
            Col.metric,
            Col.forecast_date,
            Col.loss,
            *WIDE_DIAGNOSTIC_COLUMNS,
        ]
    )
    dialsup_diagnostics = ", ".join(
        f"NULL AS {qid(column)}" for column in WIDE_DIAGNOSTIC_COLUMNS
    )
    dialsup_columns = ", ".join(
        qid(column)
        for column in [
            *WIDE_IDENTITY_DIMENSIONS,
            Col.metric,
            Col.forecast_date,
            Col.loss,
        ]
    )
    return f"""
        SELECT {columns} FROM read_parquet({qlit(ylt_path)}) WHERE {qid(Col.metric)} = 'euws_override'
        UNION ALL
        SELECT {dialsup_columns}, {dialsup_diagnostics} FROM read_parquet({qlit(dialsup_path)}) WHERE {qid(Col.metric)} = 'dialsup_localccy_forecast'
    """


def _wide_copy_query(
    ylt_path: Path, dialsup_path: Path, output_path: Path, dates: list[str]
) -> str:
    dims = ", ".join(qid(column) for column in WIDE_IDENTITY_DIMENSIONS)
    output_cols = _ordered_output_columns(dates)
    select_items = [f"{qid(column)}" for column in WIDE_IDENTITY_DIMENSIONS]
    select_items.insert(3, f"'cds_wide_analysis' AS {qid(Col.output_use)}")
    select_items.extend(
        f"MAX({qid(column)}) FILTER (WHERE {qid(column)} IS NOT NULL) AS {qid(column)}"
        for column in WIDE_DIAGNOSTIC_COLUMNS
    )
    for date in dates:
        month = date.replace("-", "")[:6]
        for metric in ["euws_override", "dialsup_localccy_forecast"]:
            column = f"{metric}_{month}_loss"
            select_items.append(
                f"MAX({qid(Col.loss)}) FILTER (WHERE {qid(Col.metric)} = {qlit(metric)} AND CAST({qid(Col.forecast_date)} AS VARCHAR) = {qlit(date)}) AS {qid(column)}"
            )
    ordered_select = ", ".join(qid(column) for column in output_cols)
    return f"""
        COPY (
            SELECT {ordered_select}
            FROM (
                SELECT {", ".join(select_items)}
                FROM ({_source_union(ylt_path, dialsup_path)})
                GROUP BY {dims}
            )
        ) TO {qlit(output_path)} (FORMAT PARQUET)
    """


def _ordered_output_columns(dates: list[str]) -> list[str]:
    columns: list[str] = list(WIDE_IDENTITY_DIMENSIONS)
    columns.insert(3, Col.output_use)
    columns.extend(WIDE_DIAGNOSTIC_COLUMNS)
    columns.extend(f"euws_override_{date.replace('-', '')[:6]}_loss" for date in dates)
    columns.extend(
        f"dialsup_localccy_forecast_{date.replace('-', '')[:6]}_loss" for date in dates
    )
    return columns


def _main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("ylt_path", type=Path)
    parser.add_argument("dialsup_path", type=Path)
    parser.add_argument("output_path", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    _write_worker(args.ylt_path, args.dialsup_path, args.output_path)


if __name__ == "__main__":
    _main()
