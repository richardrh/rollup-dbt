"""SQL Server I/O — connection probe and Hisco-fanout push.

Run via the `rollup push-to-sql` CLI subcommand — never invoked
automatically as part of `rollup run`. The pipeline writes parquets;
pushing to SQL is an explicit, separate, idempotent step.

Connection string format (Windows auth — no credentials inline):
    mssql+pyodbc://server/database?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes

SQL auth:
    mssql+pyodbc://user:pass@server/database?driver=ODBC+Driver+17+for+SQL+Server

The push uses `polars.DataFrame.write_database(if_table_exists="replace")`
— each table is dropped and recreated on every push, no DDL management
needed.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import polars as pl


log = logging.getLogger("rollup.sql_push")


@dataclass(frozen=True)
class ConnectionTestResult:
    """Outcome of `test_connection`. Never raises — caller renders."""
    ok:            bool
    version:       str | None
    database:      str | None
    schema:        str | None              # echoed back for display
    schema_exists: bool | None             # None when caller didn't ask
    error:         str | None


def test_connection(conn_str: str, *, schema: str | None = None) -> ConnectionTestResult:
    """Open a connection, query @@VERSION + DB_NAME(), optionally verify a schema.

    Read-only probe — no writes. Catches every exception and returns it
    in the result so the caller can format the error nicely.

    `schema` (when given) is checked against `sys.schemas`; the result
    has `schema_exists=True/False`.
    """
    from sqlalchemy import create_engine, text

    try:
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            version  = conn.execute(text("SELECT @@VERSION")).scalar()
            database = conn.execute(text("SELECT DB_NAME()")).scalar()
            schema_exists: bool | None = None
            if schema is not None:
                row = conn.execute(
                    text("SELECT 1 FROM sys.schemas WHERE name = :s"),
                    {"s": schema},
                ).scalar()
                schema_exists = bool(row)
        return ConnectionTestResult(
            ok=True,
            version=version,
            database=database,
            schema=schema,
            schema_exists=schema_exists,
            error=None,
        )
    except Exception as e:
        return ConnectionTestResult(
            ok=False, version=None, database=None,
            schema=schema, schema_exists=None,
            error=f"{type(e).__name__}: {e}",
        )


# Glob for the parquets we push: only the Hisco fanout files. The
# long-format combined audit and the --dump-interim debug parquets are
# 21M+ rows and stay parquet-only by design.
_PUSH_GLOB = "Hisco*.parquet"


def list_pushable_parquets(output_dir: Path) -> list[Path]:
    """Return the Hisco fanout parquets present in `output_dir`, sorted."""
    if not output_dir.exists():
        return []
    return sorted(output_dir.glob(_PUSH_GLOB))


def push_parquet_to_sql(
    parquet: Path,
    *,
    conn_str: str,
    schema: str | None = None,
) -> int:
    """Read `parquet` and write it to a SQL Server table.

    The table name is the parquet filename without the `.parquet` suffix
    (e.g. `HiscoAIR_202601_main.parquet` → table `HiscoAIR_202601_main`).
    `if_table_exists="replace"` drops + recreates each table.

    Returns the number of rows pushed.
    """
    df = pl.read_parquet(parquet)
    table_name = parquet.stem
    fq_name = f"{schema}.{table_name}" if schema else table_name

    df.write_database(
        table_name=fq_name,
        connection=conn_str,
        if_table_exists="replace",
        engine="sqlalchemy",
    )
    log.info(f"sql: wrote {df.height:,} rows → {fq_name}")
    return df.height
