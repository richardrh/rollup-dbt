from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import tomllib
from typing import Literal

import polars as pl


SqlCheckStatus = Literal["SKIPPED", "OK", "FAIL"]
SqlIfExists = Literal["replace", "append", "fail"]

_VALID_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_VALID_IF_EXISTS: set[str] = {"replace", "append", "fail"}


@dataclass(frozen=True)
class SqlConfig:
    connection_string: str | None = None
    schema: str = "dbo"
    if_exists: SqlIfExists = "replace"
    table_prefix: str = ""


@dataclass(frozen=True)
class RollupConfig:
    sql: SqlConfig


@dataclass(frozen=True)
class SqlCheckResult:
    status: SqlCheckStatus
    message: str


def load_rollup_config(config_path: Path | str = "rollup.local.toml") -> RollupConfig:
    path = Path(config_path)
    payload: dict[str, object]
    if path.is_file():
        with path.open("rb") as file:
            payload = tomllib.load(file)
    else:
        payload = {}

    sql_payload = payload.get("sql") or {}
    if not isinstance(sql_payload, dict):
        raise ValueError("[sql] config must be a TOML table")

    connection_string = sql_payload.get("connection_string") or sql_payload.get(
        "mssql_conn_str"
    )
    if connection_string is not None and not isinstance(connection_string, str):
        raise ValueError("[sql].connection_string must be a string")

    schema = sql_payload.get("schema", "dbo")
    if_exists = sql_payload.get("if_exists", "replace")
    table_prefix = sql_payload.get("table_prefix", "")

    if not isinstance(schema, str):
        raise ValueError("[sql].schema must be a string")
    if not isinstance(if_exists, str):
        raise ValueError("[sql].if_exists must be a string")
    if not isinstance(table_prefix, str):
        raise ValueError("[sql].table_prefix must be a string")

    sql_config = SqlConfig(
        connection_string=connection_string,
        schema=schema,
        if_exists=_validate_if_exists(if_exists),
        table_prefix=table_prefix,
    )
    validate_sql_config(sql_config, require_connection=False)
    return RollupConfig(sql=sql_config)


def check_sql_connection(config_path: Path | str = "rollup.local.toml") -> SqlCheckResult:
    try:
        config = load_rollup_config(config_path).sql
        if not config.connection_string:
            return SqlCheckResult(
                "SKIPPED",
                f"No SQL connection string configured in {Path(config_path)}.",
            )

        engine = _create_sqlalchemy_engine(config.connection_string)
        try:
            with engine.connect() as connection:
                connection.exec_driver_sql("SELECT 1")
        finally:
            dispose = getattr(engine, "dispose", None)
            if dispose is not None:
                dispose()

        return SqlCheckResult("OK", "SQL Server connection check succeeded.")
    except Exception as exc:
        return SqlCheckResult("FAIL", f"SQL Server connection check failed: {exc}")


def require_working_sql_config(config_path: Path | str = "rollup.local.toml") -> SqlConfig:
    result = check_sql_connection(config_path)
    if result.status != "OK":
        raise RuntimeError(result.message)
    return load_rollup_config(config_path).sql


def push_mart_parquets_to_sql(output_root: Path | str, sql_config: SqlConfig) -> list[str]:
    validate_sql_config(sql_config, require_connection=True)
    output_root = Path(output_root)
    mart_paths = sorted((output_root / "marts").glob("*.parquet"))
    if not mart_paths:
        return []

    engine = _create_sqlalchemy_engine(sql_config.connection_string or "")
    pushed_tables: list[str] = []
    try:
        for parquet_path in mart_paths:
            qualified_table = qualified_table_name(sql_config, parquet_path)
            frame = pl.read_parquet(parquet_path)
            frame.write_database(
                qualified_table,
                connection=engine,
                if_table_exists=sql_config.if_exists,
            )
            pushed_tables.append(qualified_table)
    finally:
        dispose = getattr(engine, "dispose", None)
        if dispose is not None:
            dispose()

    return pushed_tables


def validate_sql_config(
    sql_config: SqlConfig,
    *,
    require_connection: bool,
) -> None:
    if require_connection and not sql_config.connection_string:
        raise ValueError("SQL connection string is required")
    validate_identifier(sql_config.schema, "schema")
    if sql_config.table_prefix:
        validate_identifier(sql_config.table_prefix, "table_prefix")
    _validate_if_exists(sql_config.if_exists)


def validate_identifier(identifier: str, field_name: str) -> str:
    if not _VALID_IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(
            f"Invalid SQL {field_name} {identifier!r}; use letters, numbers, and "
            "underscores, and start with a letter or underscore."
        )
    return identifier


def qualified_table_name(sql_config: SqlConfig, parquet_path: Path | str) -> str:
    parquet_path = Path(parquet_path)
    table_name = f"{sql_config.table_prefix}{parquet_path.stem}"
    validate_identifier(table_name, "table name")
    return f"{sql_config.schema}.{table_name}"


def _validate_if_exists(value: str) -> SqlIfExists:
    if value not in _VALID_IF_EXISTS:
        raise ValueError("[sql].if_exists must be one of: append, fail, replace")
    return value  # type: ignore[return-value]


def _create_sqlalchemy_engine(connection_string: str):
    from sqlalchemy import create_engine

    return create_engine(connection_string)
