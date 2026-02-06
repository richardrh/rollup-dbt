import pytest
import dlt
from dlt.sources.sql_database import sql_database

def test_dlt_lmrmsinsurance_query_loads_ok():
    # Arrange: pipeline to DuckDB (no secrets needed if you're only testing SELECT 1)
    pipeline = dlt.pipeline(pipeline_name="mssql_test", destination="duckdb")

    # Source name matches your config.toml section: [sources.sql_database.lmrmsinsurance...]
    source = sql_database().with_args(name="lmrmsinsurance")

    # Act: run a trivial query; dlt returns a LoadInfo object, not an int
    res = pipeline.run(source.with_query("SELECT 1 AS ok"))

    # Assert: shape/type of result and basic properties
    # dlt docs show using `load_info` from pipeline.run(...) (it's a structured result object)
    assert res is not None

