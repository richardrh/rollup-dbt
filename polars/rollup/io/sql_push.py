"""SQL Server I/O — connection probe and Hisco-fanout push.

Run via the `rollup push-to-sql` CLI subcommand — never invoked
automatically as part of `rollup run`. The pipeline writes parquets;
pushing to SQL is an explicit, separate, idempotent step.

Connection string format (Windows auth — no credentials inline):
    mssql+pyodbc://server/database?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes

SQL auth:
    mssql+pyodbc://user:pass@server/database?driver=ODBC+Driver+17+for+SQL+Server

The push uses a pure-polars path: we build the table via sqlalchemy DDL
from the polars schema, then bulk-INSERT via chunked `to_dicts()` with
`fast_executemany=True` on the engine.  This avoids the pandas dependency
that `polars.write_database(engine="sqlalchemy")` would incur.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl


if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

log = logging.getLogger("rollup.sql_push")

# Rows sent to SQL Server per execute() call.  Large enough to amortise
# network round-trips; small enough to cap Python dict memory.
_CHUNK_SIZE = 50_000


@dataclass(frozen=True)
class ConnectionTestResult:
    """Outcome of `test_connection`. Never raises — caller renders."""
    ok:            bool
    version:       str | None
    database:      str | None
    schema:        str | None              # echoed back for display
    schema_exists: bool | None             # None when caller didn't ask
    error:         str | None


def make_engine(conn_str: str) -> "Engine":
    """Create a SQLAlchemy engine for SQL Server with optimal push settings.

    `fast_executemany=True` instructs pyodbc to batch all parameter rows
    into a single network send per `execute()` call, giving a 10-50x
    speedup over the default one-row-per-round-trip behaviour.

    The caller is responsible for calling `engine.dispose()` when done.
    """
    from sqlalchemy import create_engine

    return create_engine(conn_str, fast_executemany=True)


def test_connection(conn_str: str, *, schema: str | None = None) -> ConnectionTestResult:
    """Open a connection, query @@VERSION + DB_NAME(), optionally verify a schema.

    Read-only probe — no writes. Catches every exception and returns it
    in the result so the caller can format the error nicely.

    `schema` (when given) is checked against `sys.schemas`; the result
    has `schema_exists=True/False`.
    """
    from sqlalchemy import text

    try:
        engine = make_engine(conn_str)
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

    Numeric and date/time types map to generic sqlalchemy types that
    sqlalchemy translates per dialect (Integer, Float, DateTime, etc.).

    Strings and any unrecognised container types (List, Struct, Decimal)
    fall through to NVARCHAR(MAX) — the SQL Server native Unicode type.
    VARCHAR would silently corrupt non-ASCII content; NVARCHAR(MAX) is
    the safe, forward-compatible default.
    """
    from sqlalchemy import BigInteger, Boolean, Date, DateTime, Float, Integer
    from sqlalchemy.dialects.mssql import NVARCHAR

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
    # NVARCHAR(MAX) — Unicode-safe, forward-compatible fallback.
    return NVARCHAR(length=None)


def push_parquet_to_sql(
    parquet: Path,
    *,
    engine: "Engine | None" = None,
    conn_str: str | None = None,
    schema: str | None = None,
) -> int:
    """Read `parquet` and write it to a SQL Server table.

    The table name is the parquet filename without the `.parquet` suffix
    (e.g. `HiscoAIR_202601_main.parquet` → table `HiscoAIR_202601_main`).
    Drops + recreates each table.

    Pass `engine` (preferred) when pushing multiple files so a single
    connection pool is shared across all calls.  If `engine` is None,
    `conn_str` is required and a temporary engine is created and disposed
    at the end of this call.

    Implementation note — this is **pure-polars**: we avoid
    `polars.write_database(engine="sqlalchemy")` because that path
    requires pandas (polars converts via `df.to_pandas().to_sql()`).
    Instead we build the table with sqlalchemy types from the polars
    schema, then bulk-INSERT polars rows directly via chunked `to_dicts()`
    with `fast_executemany=True` on the engine.  ADBC would be the other
    no-pandas option, but no ADBC driver exists for SQL Server.

    Note: if the INSERT fails partway, the previous table state is also
    rolled back along with the partial insert — there is no recovery, just
    no table. Re-run to recover. (Atomic swap via a staging table is a
    future improvement.)

    Returns the number of rows pushed.
    """
    from sqlalchemy import Column, MetaData, Table, inspect

    if engine is None and conn_str is None:
        raise ValueError("push_parquet_to_sql: supply either `engine` or `conn_str`")

    _own_engine = engine is None
    if _own_engine:
        engine = make_engine(conn_str)  # type: ignore[arg-type]

    df = pl.read_parquet(parquet)
    table_name = parquet.stem

    columns = [
        Column(name, _polars_to_sqlalchemy_type(dtype))
        for name, dtype in df.schema.items()
    ]
    metadata = MetaData()
    table = Table(table_name, metadata, *columns, schema=schema)

    try:
        with engine.begin() as conn:
            if inspect(conn).has_table(table_name, schema=schema):
                table.drop(conn)
            table.create(conn)
            # Intentional escape from polars-typed world: SQL Server has no
            # ADBC driver, and pl.write_database(engine="sqlalchemy") requires
            # pandas. Chunked to_dicts + fast_executemany is the fastest
            # pure-polars-input path available.
            for chunk in df.iter_slices(n_rows=_CHUNK_SIZE):
                rows = chunk.to_dicts()
                if rows:
                    conn.execute(table.insert(), rows)
    finally:
        if _own_engine:
            engine.dispose()

    fq_name = f"{schema}.{table_name}" if schema else table_name
    log.info(f"sql: wrote {df.height:,} rows → {fq_name}")
    return df.height
