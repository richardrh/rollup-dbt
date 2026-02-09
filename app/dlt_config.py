"""Builds the DLT runtime config from CLI input."""

from __future__ import annotations

from pathlib import Path


def generate_minimal_dlt_config(
    database: str, table: str, source_name: str = "default"
) -> None:
    """Generate a minimal DLT config with database and table from CLI input."""
    template = f"""# This file is generated from CLI input

[runtime]
log_level = "WARNING"
dlthub_telemetry = false

[sources.sql_database.{source_name}.credentials]
drivername = "mssql+pyodbc"
host = "localhost"
database = "{database}"
schema = "catmodel"
table = "{table}"
driver = "ODBC Driver 18 for SQL Server"
Encrypt = "yes"
TrustServerCertificate = "yes"
authentication = "ActiveDirectoryIntegrated"
"""
    generated_path = Path(".dlt") / "config.toml"
    generated_path.parent.mkdir(parents=True, exist_ok=True)
    generated_path.write_text(template.strip() + "\n")


# Legacy function for backward compatibility
def generate_dlt_config(sources: list[dict[str, any]] | None = None) -> None:
    """Legacy function - use generate_minimal_dlt_config instead."""
    if sources is None:
        # Generate with default values if no sources provided
        generate_minimal_dlt_config("master", "", "default")
    else:
        # Handle legacy sources format
        for source in sources:
            if source.get("connector") == "sql_database":
                params = source.get("params", {})
                database = params.get("database", "master")
                table = params.get("table", "")
                generate_minimal_dlt_config(
                    database, table, source.get("name", "default")
                )
                break
