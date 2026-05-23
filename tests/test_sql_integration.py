from __future__ import annotations

from pathlib import Path
import time

import polars as pl
import pytest
from sqlalchemy import URL, create_engine, text

from rollup.sql import SqlConfig, push_mart_parquets_to_sql


SQL_SERVER_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest"
SA_PASSWORD = "Fundsaurus!SqlPush2026"


pytestmark = pytest.mark.integration


def test_push_mart_parquets_to_real_sql_server_container(tmp_path: Path) -> None:
    driver = _sql_server_odbc_driver_or_skip()

    output_root = tmp_path / "output"
    marts_dir = output_root / "marts"
    marts_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "id": [1, 2],
            "label": ["alpha", "beta"],
            "amount": [10.5, 20.25],
        }
    ).write_parquet(marts_dir / "HiscoAIR_202601_main.parquet")

    container = _start_sql_server_container()
    try:
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(1433))
        connection_url = _connection_url(host=host, port=port, driver=driver)
        engine = create_engine(connection_url)
        try:
            _wait_for_sql_server(engine)
            pushed_tables = push_mart_parquets_to_sql(
                output_root,
                SqlConfig(
                    connection_string=str(connection_url),
                    schema="dbo",
                    if_exists="replace",
                    table_prefix="it_",
                ),
            )

            assert pushed_tables == ["dbo.it_HiscoAIR_202601_main"]
            with engine.connect() as connection:
                assert connection.execute(
                    text(
                        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                        "WHERE TABLE_SCHEMA = 'dbo' "
                        "AND TABLE_NAME = 'it_HiscoAIR_202601_main'"
                    )
                ).scalar_one() == 1
                rows = connection.execute(
                    text(
                        "SELECT id, label, amount "
                        "FROM dbo.it_HiscoAIR_202601_main "
                        "ORDER BY id"
                    )
                ).all()

            assert [(row.id, row.label, float(row.amount)) for row in rows] == [
                (1, "alpha", 10.5),
                (2, "beta", 20.25),
            ]
        finally:
            engine.dispose()
    finally:
        container.stop()


def _sql_server_odbc_driver_or_skip() -> str:
    try:
        import pyodbc
    except Exception as exc:
        pytest.skip(f"pyodbc/ODBC is unavailable: {exc}")

    try:
        drivers = pyodbc.drivers()
    except Exception as exc:
        pytest.skip(f"Could not enumerate ODBC drivers: {exc}")

    for driver in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"):
        if driver in drivers:
            return driver
    pytest.skip(
        "No Microsoft SQL Server ODBC driver found; install ODBC Driver 18 "
        "or 17 for SQL Server to run this integration test."
    )


def _start_sql_server_container():
    try:
        from testcontainers.core.container import DockerContainer

        container = (
            DockerContainer(SQL_SERVER_IMAGE)
            .with_env("ACCEPT_EULA", "Y")
            .with_env("MSSQL_SA_PASSWORD", SA_PASSWORD)
            .with_env("MSSQL_PID", "Developer")
            .with_exposed_ports(1433)
        )
        container.start()
    except Exception as exc:
        pytest.skip(f"Could not start SQL Server Docker container: {exc}")
    return container


def _connection_url(*, host: str, port: int, driver: str) -> URL:
    return URL.create(
        "mssql+pyodbc",
        username="sa",
        password=SA_PASSWORD,
        host=host,
        port=port,
        database="master",
        query={
            "driver": driver,
            "Encrypt": "yes",
            "TrustServerCertificate": "yes",
        },
    )


def _wait_for_sql_server(engine, *, timeout_seconds: int = 120) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return
        except Exception as exc:
            last_error = exc
            time.sleep(2)
    raise AssertionError(f"SQL Server container did not become ready: {last_error}")
