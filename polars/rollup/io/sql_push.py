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


def _polars_to_sqlalchemy_type(dtype: pl.DataType):
    """Map a polars dtype to a sqlalchemy column type.

    Generic types (Integer, BigInteger, Float, String, Boolean, Date,
    DateTime) — sqlalchemy translates them per dialect. NVARCHAR(MAX)
    on MSSQL, VARCHAR on PostgreSQL, etc. Containers (List/Struct) and
    Decimal aren't expected in Hisco fanouts and fall through to a wide
    `String` for forward compat.
    """
    from sqlalchemy import (
        BigInteger, Boolean, Date, DateTime, Float, Integer, String,
    )
    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.UInt8, pl.UInt16):
        return Integer()
    if dtype in (pl.Int64, pl.UInt32, pl.UInt64):
        return BigInteger()
    if dtype in (pl.Float32, pl.Float64):
        return Float()
    if dtype == pl.Boolean:
        return Boolean()
    if dtype == pl.Date:
        return Date()
    if dtype.base_type() == pl.Datetime:
        return DateTime()
    return String(length=4000)


def push_parquet_to_sql(
    parquet: Path,
    *,
    conn_str: str,
    schema: str | None = None,
) -> int:
    """Read `parquet` and write it to a SQL Server table.

    The table name is the parquet filename without the `.parquet` suffix
    (e.g. `HiscoAIR_202601_main.parquet` → table `HiscoAIR_202601_main`).
    Drops + recreates each table.

    Implementation note — this is **pure-polars**: we avoid
    `polars.write_database(engine="sqlalchemy")` because that path
    requires pandas (polars converts via `df.to_pandas().to_sql()`).
    Instead we build the table with sqlalchemy types from the polars
    schema, then bulk-INSERT polars rows directly. ADBC would be the
    other no-pandas option, but no ADBC driver exists for SQL Server.

    Returns the number of rows pushed.
    """
    from sqlalchemy import (
        Column, MetaData, Table, create_engine, inspect,
    )

    df = pl.read_parquet(parquet)
    table_name = parquet.stem

    columns = [
        Column(name, _polars_to_sqlalchemy_type(dtype))
        for name, dtype in df.schema.items()
    ]
    metadata = MetaData()
    table = Table(table_name, metadata, *columns, schema=schema)

    engine = create_engine(conn_str)
    with engine.begin() as conn:
        if inspect(conn).has_table(table_name, schema=schema):
            table.drop(conn)
        table.create(conn)
        rows = df.to_dicts()
        if rows:
            conn.execute(table.insert(), rows)

    fq_name = f"{schema}.{table_name}" if schema else table_name
    log.info(f"sql: wrote {df.height:,} rows → {fq_name}")
    return df.height
