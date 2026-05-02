"""Integration tests: real SQL Server in Docker.

Skipped by default. Run with:

    uv run pytest --run-integration
    # or
    uv run pytest -m integration

Requirements:
- Docker daemon running on the host.
- ~2 GB image pull on first run (Microsoft mssql/server).
- ~30-60s container boot per test session (we use a session-scoped fixture).

The container is torn down on session exit. No state on the host filesystem.

These tests use `pymssql` (pure-Python driver bundled with freetds) so the
runner doesn't need a system ODBC install. Production uses pyodbc; the SQL
content of `test_connection` and `push_parquet_to_sql` is dialect-stable
enough that pymssql is a faithful proxy for the integration check.
"""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest


pytestmark = pytest.mark.integration


# --------------------------------------------------------------------------- #
# Fixtures                                                                    #
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="session")
def mssql_url():
    """Spin up a SQL Server container; yield a sqlalchemy URL using pymssql.

    Session-scoped — boots once per `pytest` invocation.
    """
    try:
        from testcontainers.mssql import SqlServerContainer
    except ImportError:
        pytest.skip("testcontainers[mssql] not installed")

    # `mcr.microsoft.com/mssql/server:2022-latest` is ~2 GB. The container's
    # default port is 1433; testcontainers maps it to a random host port and
    # builds a connection URL for us. We override the dialect to pymssql so
    # the test process doesn't need an ODBC driver on the host.
    container = SqlServerContainer("mcr.microsoft.com/mssql/server:2022-latest")
    try:
        container.start()
    except Exception as e:
        pytest.skip(f"could not start SQL Server container: {e}")
    try:
        # Default URL is mssql+pyodbc://...; rewrite to pymssql.
        url = container.get_connection_url()
        if url.startswith("mssql+pyodbc://"):
            url = "mssql+pymssql://" + url[len("mssql+pyodbc://"):]
            # pymssql doesn't accept the ?driver=... query string pyodbc uses.
            url = url.split("?", 1)[0]
        yield url
    finally:
        container.stop()


@pytest.fixture
def tiny_parquet(tmp_path: Path) -> Path:
    """A 3-row Hisco-shaped parquet for round-trip testing."""
    df = pl.DataFrame({
        "ModelEventID":              [1, 2, 3],
        "ModelYear":                 [100, 200, 300],
        "CurrencyCode":              ["GBP", "GBP", "EUR"],
        "ModelYOA":                  [0, 0, 0],
        "ModelGrossLoss":            [1234.5, 2345.6, 3456.7],
        "ModelInwardsReinstatement": [0, 0, 0],
        "ModelEventDay":             [1, 30, 200],
        "LossClassName":             ["UK Household", "UK Household", "EU Spec Property"],
    })
    out = tmp_path / "HiscoTEST_dialsup.parquet"
    df.write_parquet(out)
    return out


# --------------------------------------------------------------------------- #
# test_connection — read-only probe                                           #
# --------------------------------------------------------------------------- #

def test_test_connection_succeeds_against_live_server(mssql_url: str):
    """`test_connection` connects to a live SQL Server and reports version + db."""
    from rollup.io.sql_push import test_connection

    result = test_connection(mssql_url)
    assert result.ok, f"connection failed: {result.error}"
    assert result.version is not None and "Microsoft SQL Server" in result.version
    assert result.database is not None and len(result.database) > 0
    assert result.error is None


def test_test_connection_with_existing_schema(mssql_url: str):
    """The default `dbo` schema exists on every fresh SQL Server."""
    from rollup.io.sql_push import test_connection

    result = test_connection(mssql_url, schema="dbo")
    assert result.ok
    assert result.schema == "dbo"
    assert result.schema_exists is True


def test_test_connection_with_missing_schema(mssql_url: str):
    """An obviously-bogus schema is reported as missing, but the probe still succeeds."""
    from rollup.io.sql_push import test_connection

    result = test_connection(mssql_url, schema="this_schema_definitely_does_not_exist_12345")
    assert result.ok    # connection ok
    assert result.schema_exists is False


def test_test_connection_returns_error_on_bad_host():
    """Bad hostname → ok=False, error populated, no exception."""
    from rollup.io.sql_push import test_connection

    result = test_connection("mssql+pymssql://nonexistent-host-99999/db")
    assert result.ok is False
    assert result.error is not None
    assert result.version is None


# --------------------------------------------------------------------------- #
# push_parquet_to_sql — round-trip                                            #
# --------------------------------------------------------------------------- #

def test_push_parquet_to_sql_round_trip(mssql_url: str, tiny_parquet: Path):
    """Push a tiny parquet, read it back via SELECT, assert row + sum match."""
    from sqlalchemy import create_engine, text
    from rollup.io.sql_push import push_parquet_to_sql

    table = tiny_parquet.stem  # "HiscoTEST_dialsup"
    rows_written = push_parquet_to_sql(tiny_parquet, conn_str=mssql_url)
    assert rows_written == 3

    engine = create_engine(mssql_url)
    with engine.connect() as conn:
        n = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        s = conn.execute(text(f"SELECT SUM(ModelGrossLoss) FROM {table}")).scalar()
    assert n == 3
    assert float(s) == pytest.approx(1234.5 + 2345.6 + 3456.7)


def test_push_parquet_replaces_existing_table(mssql_url: str, tmp_path: Path):
    """Pushing twice overwrites the first table — `if_table_exists='replace'`."""
    from sqlalchemy import create_engine, text
    from rollup.io.sql_push import push_parquet_to_sql

    # First push: 5 rows.
    df_a = pl.DataFrame({"x": [1, 2, 3, 4, 5]})
    p_a = tmp_path / "HiscoTEST_replace.parquet"
    df_a.write_parquet(p_a)
    push_parquet_to_sql(p_a, conn_str=mssql_url)

    # Second push: 2 rows in a parquet of the same name → table replaced.
    df_b = pl.DataFrame({"x": [10, 20]})
    df_b.write_parquet(p_a)
    push_parquet_to_sql(p_a, conn_str=mssql_url)

    engine = create_engine(mssql_url)
    with engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM HiscoTEST_replace")).scalar()
        s = conn.execute(text("SELECT SUM(x) FROM HiscoTEST_replace")).scalar()
    assert n == 2
    assert int(s) == 30
