"""DLT pipeline for loading Risklink ELT data from CSV."""

from __future__ import annotations

import dlt
from typing import Any
import polars as pl
from pathlib import Path

from config.loader import load_config


def load_risklink_elt_from_csv(csv_path: str | None = None) -> Any:
    """Load Risklink ELT data from CSV file to staging table."""
    config = load_config()

    csv_file = csv_path or config["sources"]["risklink_elt_csv"]

    if not Path(csv_file).exists():
        raise FileNotFoundError(f"Risklink ELT file not found: {csv_file}")

    # Create DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name="risklink_elt_load",
        destination="duckdb",  # Will be configured for SQL Server in production
        dataset_name=config["schemas"]["raw_schema"],
    )

    # Read CSV with Polars
    print(f"Loading Risklink ELT from {csv_file}...")
    df = pl.read_csv(csv_file)

    print(f"Loaded {len(df):,} rows")
    print(f"Columns: {df.columns}")

    # Basic validation
    required_cols = ["eventid", "rate", "perspvalue", "stddevi", "stddevc", "expvalue"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Convert to DLT resource
    @dlt.resource(
        name=config["staging"]["risklink_elts_table"], write_disposition="replace"
    )
    def elt_data():
        yield from df.to_dicts()

    # Run pipeline
    load_info = pipeline.run(elt_data())
    print(f"Pipeline completed: {load_info}")

    return load_info


if __name__ == "__main__":
    # Example usage
    import sys

    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
        load_risklink_elt_from_csv(csv_path)
    else:
        load_risklink_elt_from_csv()
