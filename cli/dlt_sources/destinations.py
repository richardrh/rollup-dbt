"""DLT destination configuration for SQL Server."""

from __future__ import annotations

import dlt
from dlt.destinations import mssql

from config.loader import load_config


def get_mssql_destination():
    """Get configured SQL Server destination."""
    config = load_config()

    return mssql(
        credentials={
            "drivername": "mssql+pyodbc",
            "host": config["database"]["host"],
            "database": config["database"]["database"],
            "username": None,  # Uses Windows Auth / ActiveDirectoryIntegrated
            "password": None,
            "query": {
                "driver": config["database"]["driver"],
                "TrustServerCertificate": config["database"]["TrustServerCertificate"],
                "authentication": config["database"]["authentication"],
            },
        }
    )


def create_pipeline(
    pipeline_name: str, use_mssql: bool = True, dataset_name: str = "catmodel"
) -> dlt.Pipeline:
    """
    Create a DLT pipeline with appropriate destination.

    Args:
        pipeline_name: Name of the pipeline
        use_mssql: If True, use SQL Server; otherwise use DuckDB (local testing)
        dataset_name: Schema/dataset name

    Returns:
        Configured DLT pipeline
    """
    if use_mssql:
        destination = get_mssql_destination()
    else:
        destination = "duckdb"

    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
    )
