"""Helper for storing user-defined source metadata."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import tomli_w
import tomllib

CONFIG_PATH = Path(__file__).parent / "sources.toml"

DEFAULT_CONNECTORS: dict[str, dict[str, Any]] = {
    "sql_database": {
        "type": "sql",
        "label": "Microsoft SQL Server via ODBC (pyodbc)",
        "default_section": "credentials",
        "fields": [
            {
                "name": "drivername",
                "label": "Driver name",
                "default": "mssql+pyodbc",
                "required": True,
            },
            {
                "name": "host",
                "label": "SQL host",
                "default": "localhost",
                "required": True,
            },
            {
                "name": "database",
                "label": "Database",
                "default": "master",
                "required": True,
            },
            {
                "name": "schema",
                "label": "Schema",
                "default": "catmodel",
                "required": True,
            },
            {"name": "table", "label": "Table", "default": "", "required": False},
            {
                "name": "driver",
                "label": "ODBC driver",
                "section": "query",
                "default": "ODBC Driver 18 for SQL Server",
                "required": True,
            },
            {
                "name": "Encrypt",
                "label": "Encrypt",
                "section": "query",
                "default": "yes",
                "required": True,
            },
            {
                "name": "TrustServerCertificate",
                "label": "Trust server certificate",
                "section": "query",
                "default": "yes",
                "required": True,
            },
            {
                "name": "authentication",
                "label": "Authentication mode",
                "section": "query",
                "default": "ActiveDirectoryIntegrated",
                "required": True,
            },
        ],
    },
    "parquet": {
        "type": "file",
        "label": "Parquet file path",
        "default_section": "credentials",
        "fields": [
            {"name": "file_path", "label": "Parquet file path", "required": True},
        ],
    },
}


def _default_connectors() -> dict[str, dict[str, Any]]:
    return {name: copy.deepcopy(meta) for name, meta in DEFAULT_CONNECTORS.items()}


def _load_raw() -> dict[str, Any]:
    if CONFIG_PATH.exists():
        raw = tomllib.loads(CONFIG_PATH.read_text())
    else:
        raw = {}
    connectors = raw.get("connectors")
    if connectors is None:
        connectors = _default_connectors()
    raw["connectors"] = connectors
    raw.setdefault("sources", [])
    return raw


def load_config() -> dict[str, Any]:
    return _load_raw()


def save_config(config: dict[str, Any]) -> None:
    CONFIG_PATH.write_text(tomli_w.dumps(config))


def list_sources() -> list[dict[str, Any]]:
    return load_config().get("sources", [])


def add_source(source: dict[str, Any]) -> None:
    config = load_config()
    sources = config.setdefault("sources", [])
    sources[:] = [s for s in sources if s.get("name") != source.get("name")]
    sources.append(source)
    save_config(config)


def load_connectors() -> dict[str, dict[str, Any]]:
    return load_config().get("connectors", {})


def get_connector(name: str) -> dict[str, Any]:
    connectors = load_connectors()
    if name not in connectors:
        raise KeyError(f"Connector '{name}' is not registered.")
    return connectors[name]
