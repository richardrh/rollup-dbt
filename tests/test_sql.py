from __future__ import annotations

from pathlib import Path

import pytest

from rollup import sql
from rollup.sql import SqlConfig


def test_load_rollup_config_prefers_connection_string(tmp_path: Path) -> None:
    config_path = tmp_path / "rollup.local.toml"
    config_path.write_text(
        """
[sql]
connection_string = "mssql+pyodbc://new"
mssql_conn_str = "mssql+pyodbc://legacy"
schema = "reporting"
if_exists = "append"
table_prefix = "rollup_"
""".strip(),
        encoding="utf-8",
    )

    config = sql.load_rollup_config(config_path)

    assert config.sql == SqlConfig(
        connection_string="mssql+pyodbc://new",
        schema="reporting",
        if_exists="append",
        table_prefix="rollup_",
    )


def test_load_rollup_config_supports_legacy_mssql_conn_str(tmp_path: Path) -> None:
    config_path = tmp_path / "rollup.local.toml"
    config_path.write_text(
        """
[sql]
mssql_conn_str = "mssql+pyodbc://legacy"
""".strip(),
        encoding="utf-8",
    )

    config = sql.load_rollup_config(config_path)

    assert config.sql.connection_string == "mssql+pyodbc://legacy"
    assert config.sql.schema == "dbo"
    assert config.sql.if_exists == "replace"
    assert config.sql.table_prefix == ""


def test_check_sql_connection_statuses_use_sqlalchemy_engine(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    skipped = sql.check_sql_connection(tmp_path / "missing.toml")
    assert skipped.status == "SKIPPED"

    config_path = tmp_path / "rollup.local.toml"
    config_path.write_text(
        '[sql]\nconnection_string = "mssql+pyodbc://server/database"\n',
        encoding="utf-8",
    )

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback) -> None:
            return None

        def exec_driver_sql(self, statement: str) -> None:
            assert statement == "SELECT 1"

    class Engine:
        disposed = False

        def connect(self) -> Connection:
            return Connection()

        def dispose(self) -> None:
            self.disposed = True

    created_connections: list[str] = []

    def create_engine(connection_string: str) -> Engine:
        created_connections.append(connection_string)
        return Engine()

    monkeypatch.setattr(sql, "_create_sqlalchemy_engine", create_engine)

    ok = sql.check_sql_connection(config_path)
    assert ok.status == "OK"
    assert created_connections == ["mssql+pyodbc://server/database"]

    def fail_create_engine(connection_string: str) -> Engine:
        raise RuntimeError(f"cannot connect to {connection_string}")

    monkeypatch.setattr(sql, "_create_sqlalchemy_engine", fail_create_engine)
    failed = sql.check_sql_connection(config_path)
    assert failed.status == "FAIL"
    assert "cannot connect" in failed.message


def test_push_mart_parquets_to_sql_only_pushes_mart_parquets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "output"
    marts_dir = output_root / "marts"
    marts_dir.mkdir(parents=True)
    (marts_dir / "HiscoAIR_202601_main.parquet").write_text("not real parquet")
    (marts_dir / "HiscoRMS_202601_dialsup.parquet").write_text("not real parquet")
    (marts_dir / "ignore.csv").write_text("not parquet")
    (output_root / "root_level.parquet").write_text("not a mart")

    class Engine:
        def dispose(self) -> None:
            return None

    monkeypatch.setattr(sql, "_create_sqlalchemy_engine", lambda connection_string: Engine())

    read_paths: list[Path] = []
    writes: list[tuple[str, object, str]] = []

    class Frame:
        def __init__(self, path: Path) -> None:
            self.path = path

        def write_database(
            self,
            table_name: str,
            *,
            connection: object,
            if_table_exists: str,
        ) -> None:
            writes.append((table_name, connection, if_table_exists))

    def read_parquet(path: Path) -> Frame:
        read_paths.append(path)
        return Frame(path)

    monkeypatch.setattr(sql.pl, "read_parquet", read_parquet)

    pushed = sql.push_mart_parquets_to_sql(
        output_root,
        SqlConfig(
            connection_string="mssql+pyodbc://server/database",
            schema="reporting",
            if_exists="append",
            table_prefix="rollup_",
        ),
    )

    assert read_paths == [
        marts_dir / "HiscoAIR_202601_main.parquet",
        marts_dir / "HiscoRMS_202601_dialsup.parquet",
    ]
    assert pushed == [
        "reporting.rollup_HiscoAIR_202601_main",
        "reporting.rollup_HiscoRMS_202601_dialsup",
    ]
    assert [write[0] for write in writes] == pushed
    assert {write[2] for write in writes} == {"append"}


@pytest.mark.parametrize(
    ("config", "path"),
    [
        (SqlConfig(connection_string="x", schema="bad-schema"), Path("good.parquet")),
        (SqlConfig(connection_string="x", table_prefix="bad-prefix"), Path("good.parquet")),
        (SqlConfig(connection_string="x"), Path("bad-name.parquet")),
        (SqlConfig(connection_string="x"), Path("1_bad.parquet")),
    ],
)
def test_sql_identifier_validation_rejects_unsafe_values(
    config: SqlConfig,
    path: Path,
) -> None:
    with pytest.raises(ValueError):
        sql.validate_sql_config(config, require_connection=True)
        sql.qualified_table_name(config, path)
