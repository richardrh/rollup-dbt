from __future__ import annotations
# mypy: ignore-errors

import json
import logging
import subprocess
import sys
import time
from pathlib import Path

import polars as pl

from rollup.columns import Col
from rollup.marts.mart_wide import _mts_output_dimensions, _ordered_mts_wide_columns, _with_metric_output_use
from rollup.pipeline_utils import _sql_identifier, _sql_literal
from rollup.writers.parquet import write_parquet_with_log


logger = logging.getLogger(__name__)


def _write_combined_outputs(
    output_root: Path,
    ylt: pl.LazyFrame,
    ylt_dialsup: pl.LazyFrame,
) -> None:
    ylt_columns = ylt.collect_schema().names()
    dialsup_columns = ylt_dialsup.collect_schema().names()
    ylt_path = output_root / "mts_tbl_ylt_combined_all_factors.parquet"
    dialsup_path = output_root / "mts_tbl_ylt_dialsup.parquet"
    write_parquet_with_log(
        _with_metric_output_use(ylt, final_metric="euws_override", final_output_use="cds_main"),
        ylt_path,
    )
    write_parquet_with_log(
        _with_metric_output_use(
            ylt_dialsup.filter(pl.col(Col.metric) == "dialsup_localccy_forecast"),
            final_metric="dialsup_localccy_forecast",
            final_output_use="cds_dialsup",
        ),
        dialsup_path,
    )

    dims = _mts_output_dimensions(pl.DataFrame(schema={column: pl.Null for column in ylt_columns}))
    diagnostic_cols = [
        Col.risklink_blended_contribution,
        Col.verisk_blended_contribution,
        Col.uplift_factor_on_base_model,
    ]
    _write_wide_output_duckdb_subprocess(
        output_path=output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet",
        ylt_path=ylt_path,
        dialsup_path=dialsup_path,
        dims=dims,
        diagnostic_cols=[col for col in diagnostic_cols if col in ylt_columns],
        dialsup_columns=dialsup_columns,
    )


def _write_wide_output_duckdb_subprocess(
    output_path: Path,
    ylt_path: Path,
    dialsup_path: Path,
    dims: list[str],
    diagnostic_cols: list[str],
    *,
    dialsup_columns: list[str],
) -> None:
    payload = {
        "output_path": str(output_path),
        "ylt_path": str(ylt_path),
        "dialsup_path": str(dialsup_path),
        "dims": dims,
        "diagnostic_cols": diagnostic_cols,
        "dialsup_columns": dialsup_columns,
    }
    code = """
import json
import logging
from pathlib import Path
from rollup.writers.wide_outputs import _write_wide_output_duckdb

logging.basicConfig(level=logging.INFO)
payload = json.loads(__import__('sys').argv[1])
_write_wide_output_duckdb(
    Path(payload['output_path']),
    Path(payload['ylt_path']),
    Path(payload['dialsup_path']),
    payload['dims'],
    payload['diagnostic_cols'],
    dialsup_columns=payload['dialsup_columns'],
)
"""
    subprocess.run([sys.executable, "-c", code, json.dumps(payload)], check=True)


def _write_wide_output_duckdb(
    output_path: Path,
    ylt_path: Path,
    dialsup_path: Path,
    dims: list[str],
    diagnostic_cols: list[str],
    *,
    dialsup_columns: list[str],
) -> None:
    import duckdb

    started = time.perf_counter()
    logger.info("building wide output=%s", output_path, extra={"event": "wide_output_start", "path": output_path})
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.unlink(missing_ok=True)

    dim_select = ", ".join(_sql_identifier(col) for col in dims)
    dialsup_dim_select = ", ".join(
        _sql_identifier(col) if col in dialsup_columns else f"NULL AS {_sql_identifier(col)}"
        for col in dims
    )
    con = duckdb.connect()
    row_count = 0
    try:
        temp_dir = output_path.parent / ".duckdb_tmp"
        temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory={_sql_literal(temp_dir)}")
        con.execute("SET memory_limit='8GB'")
        forecast_dates = [
            row[0]
            for row in con.execute(
                f"""
                SELECT DISTINCT CAST({_sql_identifier(Col.forecast_date)} AS VARCHAR) AS forecast_date
                FROM (
                    SELECT {_sql_identifier(Col.forecast_date)}
                    FROM read_parquet({_sql_literal(ylt_path)})
                    WHERE {_sql_identifier(Col.metric)} = 'euws_override'
                    UNION ALL
                    SELECT {_sql_identifier(Col.forecast_date)}
                    FROM read_parquet({_sql_literal(dialsup_path)})
                    WHERE {_sql_identifier(Col.metric)} = 'dialsup_localccy_forecast'
                )
                WHERE forecast_date IS NOT NULL
                ORDER BY forecast_date
                """
            ).fetchall()
        ]
        loss_column_names: list[str] = []
        con.execute(
            f"""
            CREATE TEMP TABLE wide_output AS
            SELECT DISTINCT {dim_select}
            FROM read_parquet({_sql_literal(ylt_path)})
            WHERE {_sql_identifier(Col.metric)} = 'euws_override'
            UNION
            SELECT DISTINCT {dialsup_dim_select}
            FROM read_parquet({_sql_literal(dialsup_path)})
            WHERE {_sql_identifier(Col.metric)} = 'dialsup_localccy_forecast'
            """
        )
        join_condition = " AND ".join(
            f"w.{_sql_identifier(col)} IS NOT DISTINCT FROM c.{_sql_identifier(col)}" for col in dims
        )
        for forecast_date in forecast_dates:
            month = forecast_date.replace("-", "")[:6]
            date_predicate = f"CAST({_sql_identifier(Col.forecast_date)} AS VARCHAR) = {_sql_literal(forecast_date)}"
            for metric, source_path, source_dims in [
                ("euws_override", ylt_path, dim_select),
                ("dialsup_localccy_forecast", dialsup_path, dialsup_dim_select),
            ]:
                column_name = f"{metric}_{month}_loss"
                table_name = "wide_col_current"
                con.execute(
                    f"""
                    CREATE TEMP TABLE {_sql_identifier(table_name)} AS
                    SELECT
                        {source_dims},
                        ANY_VALUE({_sql_identifier(Col.loss)}) AS {_sql_identifier(column_name)}
                    FROM read_parquet({_sql_literal(source_path)})
                    WHERE {_sql_identifier(Col.metric)} = {_sql_literal(metric)}
                      AND {date_predicate}
                    GROUP BY ALL
                    """
                )
                loss_column_names.append(column_name)
                existing_columns = ", ".join(
                    f"w.{_sql_identifier(col)}" for col in [*dims, *loss_column_names[:-1]]
                )
                con.execute(
                    f"""
                    CREATE TEMP TABLE wide_next AS
                    SELECT
                        {existing_columns},
                        c.{_sql_identifier(column_name)}
                    FROM wide_output w
                    LEFT JOIN {_sql_identifier(table_name)} c
                      ON {join_condition}
                    """
                )
                con.execute("DROP TABLE wide_output")
                con.execute(f"DROP TABLE {_sql_identifier(table_name)}")
                con.execute("ALTER TABLE wide_next RENAME TO wide_output")
        if not loss_column_names:
            raise ValueError("wide output has no forecast loss columns")

        if diagnostic_cols:
            diagnostic_select = ", ".join(
                f"ANY_VALUE({_sql_identifier(col)}) FILTER (WHERE {_sql_identifier(col)} IS NOT NULL) AS {_sql_identifier(col)}"
                for col in diagnostic_cols
            )
            con.execute(
                f"""
                CREATE TEMP TABLE wide_diagnostics AS
                SELECT {dim_select}, {diagnostic_select}
                FROM read_parquet({_sql_literal(ylt_path)})
                WHERE {_sql_identifier(Col.metric)} = 'euws_override'
                GROUP BY {dim_select}
                """
            )
            diagnostic_join = " AND ".join(
                f"w.{_sql_identifier(col)} IS NOT DISTINCT FROM d.{_sql_identifier(col)}" for col in dims
            )
            diagnostic_projection = ", ".join(f"d.{_sql_identifier(col)}" for col in diagnostic_cols)
            con.execute(
                f"""
                CREATE TEMP TABLE wide_with_diagnostics AS
                SELECT w.*, {diagnostic_projection}
                FROM wide_output w
                LEFT JOIN wide_diagnostics d
                  ON {diagnostic_join}
                """
            )
            con.execute("DROP TABLE wide_output")
            con.execute("ALTER TABLE wide_with_diagnostics RENAME TO wide_output")

        con.execute(
            f"""
            CREATE TEMP TABLE wide_with_output_use AS
            SELECT *, 'cds_wide_analysis' AS {_sql_identifier(Col.output_use)}
            FROM wide_output
            """
        )
        con.execute("DROP TABLE wide_output")
        con.execute("ALTER TABLE wide_with_output_use RENAME TO wide_output")

        grouped_columns = [*dims, *loss_column_names, *diagnostic_cols, Col.output_use]
        ordered_columns = _ordered_mts_wide_columns(grouped_columns)
        ordered_select = ", ".join(_sql_identifier(col) for col in ordered_columns)
        row_count = con.execute("SELECT COUNT(*) FROM wide_output").fetchone()[0]
        con.execute(f"COPY (SELECT {ordered_select} FROM wide_output) TO {_sql_literal(output_path)} (FORMAT PARQUET)")
    finally:
        con.close()

    elapsed_seconds = time.perf_counter() - started
    logger.info(
        "wrote output=%s rows=%d elapsed=%.2fs",
        output_path,
        row_count,
        elapsed_seconds,
        extra={"event": "write_output", "path": output_path, "rows": row_count, "elapsed_seconds": elapsed_seconds, "lazy": False},
    )
