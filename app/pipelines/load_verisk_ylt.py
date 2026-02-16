"""DLT pipeline for loading Verisk YLT data from CSV."""

from __future__ import annotations

import dlt
from typing import Any
import polars as pl
from pathlib import Path

from config.loader import load_config


def load_verisk_ylt_from_csv(csv_path: str | None = None) -> Any:
    """Load Verisk YLT data from CSV file to staging table."""
    # Load configuration
    config = load_config()

    # Use provided path or fall back to config
    csv_file = csv_path or config["sources"]["verisk_ylt_csv"]

    # Verify file exists
    if not Path(csv_file).exists():
        raise FileNotFoundError(f"Verisk YLT file not found: {csv_file}")

    # Create DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name="verisk_ylt_load",
        destination="duckdb",  # Will be configured for SQL Server in production
        dataset_name=config["schemas"]["raw_schema"],
    )

    # Read CSV with Polars
    print(f"Loading Verisk YLT from {csv_file}...")
    df = pl.read_csv(csv_file)

    print(f"Loaded {len(df):,} rows")
    print(f"Columns: {df.columns}")

    # Basic validation
    required_cols = ["yearid", "eventid", "net_pre_cat_loss"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"Warning: Missing expected columns: {missing_cols}")
        print(f"Available columns: {df.columns}")

    # Convert to DLT resource
    @dlt.resource(
        name=config["staging"]["verisk_ylts_table"], write_disposition="replace"
    )
    def ylt_data():
        yield from df.to_dicts()

    # Run pipeline
    load_info = pipeline.run(ylt_data())
    print(f"Pipeline completed: {load_info}")

    return load_info


if __name__ == "__main__":
    # Example usage
    import sys

    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
        load_verisk_ylt_from_csv(csv_path)
    else:
        load_verisk_ylt_from_csv()
