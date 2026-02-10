"""DLT sources for loading CSV data files."""

from __future__ import annotations

from typing import Iterator
import dlt
from dlt.sources.filesystem import filesystem

from config.loader import load_config, get_source_paths


@dlt.source(name="risklink_elt")
def risklink_elt_source(csv_path: str | None = None) -> dlt.resource:
    """
    Source for Risklink ELT (Event Loss Table) CSV files.
    """
    if csv_path is None:
        config = load_config()
        csv_path = config["sources"]["risklink_elt_csv"]

    return filesystem(
        bucket_url=csv_path,
        file_glob="*.csv",
    ) | dlt.transformer(name="elt_csv")(parse_csv)


@dlt.source(name="verisk_ylt")
def verisk_ylt_source(csv_path: str | None = None) -> dlt.resource:
    """
    Source for Verisk YLT (Year Loss Table) CSV files.
    """
    if csv_path is None:
        config = load_config()
        csv_path = config["sources"]["verisk_ylt_csv"]

    return filesystem(
        bucket_url=csv_path,
        file_glob="*.csv",
    ) | dlt.transformer(name="ylt_csv")(parse_csv)


def parse_csv(file_item) -> Iterator[dict]:
    """Parse CSV file and yield rows as dictionaries."""
    import csv
    import io

    content = file_item.read_bytes()
    text = content.decode("utf-8")

    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        # Clean up column names (lowercase, no spaces)
        cleaned = {k.lower().replace(" ", "_"): v for k, v in row.items()}
        yield cleaned


def load_risklink_elt_to_staging(csv_path: str | None = None) -> dlt.load_info:
    """Load Risklink ELT from CSV to staging table."""
    config = load_config()
    csv_path = csv_path or config["sources"]["risklink_elt_csv"]
    table_name = config["staging"]["risklink_elts_table"]

    pipeline = dlt.pipeline(
        pipeline_name="risklink_elt_import",
        destination="duckdb",  # TODO: Use SQL Server in production
        dataset_name=config["schemas"]["raw_schema"],
    )

    source = risklink_elt_source(csv_path)
    source = source.with_resources(
        dlt.resource(source.elt_csv, name=table_name, write_disposition="replace")
    )

    return pipeline.run(source)


def load_verisk_ylt_to_staging(csv_path: str | None = None) -> dlt.load_info:
    """Load Verisk YLT from CSV to staging table."""
    config = load_config()
    csv_path = csv_path or config["sources"]["verisk_ylt_csv"]
    table_name = config["staging"]["verisk_ylts_table"]

    pipeline = dlt.pipeline(
        pipeline_name="verisk_ylt_import",
        destination="duckdb",  # TODO: Use SQL Server in production
        dataset_name=config["schemas"]["raw_schema"],
    )

    source = verisk_ylt_source(csv_path)
    source = source.with_resources(
        dlt.resource(source.ylt_csv, name=table_name, write_disposition="replace")
    )

    return pipeline.run(source)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "risklink":
        print("Loading Risklink ELT...")
        info = load_risklink_elt_to_staging()
        print(f"Loaded: {info}")
    elif len(sys.argv) > 1 and sys.argv[1] == "verisk":
        print("Loading Verisk YLT...")
        info = load_verisk_ylt_to_staging()
        print(f"Loaded: {info}")
    else:
        print("Usage: python csv_sources.py [risklink|verisk]")
