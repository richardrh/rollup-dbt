"""Unit tests for the sql_push schema guard.

These tests do NOT require a real SQL Server connection — validation raises
before any DDL or INSERT is attempted.
"""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from rollup.schemas.columns import HiscoFanoutCol as H
from rollup.validate import SchemaError


def _minimal_fanout_df() -> dict:
    """Minimal valid Hisco-fanout row."""
    return {
        H.MODEL_EVENT_ID:              [1],
        H.MODEL_YEAR:                  [2026],
        H.CURRENCY_CODE:               ["GBP"],
        H.MODEL_YOA:                   [0],
        H.MODEL_GROSS_LOSS:            [100.0],
        H.MODEL_INWARDS_REINSTATEMENT: [0],
        H.MODEL_EVENT_DAY:             [0],
        H.LOSS_CLASS_NAME:             ["UK HH"],
    }


def _minimal_fanout_schema() -> pl.Schema:
    return pl.Schema({
        H.MODEL_EVENT_ID:              pl.Int64,
        H.MODEL_YEAR:                  pl.Int64,
        H.CURRENCY_CODE:               pl.String,
        H.MODEL_YOA:                   pl.Int32,
        H.MODEL_GROSS_LOSS:            pl.Float64,
        H.MODEL_INWARDS_REINSTATEMENT: pl.Int32,
        H.MODEL_EVENT_DAY:             pl.Int64,
        H.LOSS_CLASS_NAME:             pl.String,
    })


def test_push_rejects_parquet_with_extra_column(tmp_path: Path) -> None:
    """A parquet with a column not in HISCO_FANOUT is rejected before any SQL touched."""
    from rollup.io.sql_push import push_parquet_to_sql

    data = _minimal_fanout_df()
    data["rogue_extra"] = [1]   # not in HISCO_FANOUT

    bad = tmp_path / "HiscoAIR_202601_main.parquet"
    pl.DataFrame(data, schema={**_minimal_fanout_schema(), "rogue_extra": pl.Int64}).write_parquet(bad)

    with pytest.raises(SchemaError, match="rogue_extra"):
        # conn_str is never reached — validation fires first
        push_parquet_to_sql(bad, conn_str="mssql+pyodbc://unused/unused")


def test_push_rejects_parquet_with_missing_column(tmp_path: Path) -> None:
    """A parquet missing a required HISCO_FANOUT column raises SchemaError immediately."""
    from rollup.io.sql_push import push_parquet_to_sql

    data = _minimal_fanout_df()
    # Drop a required column
    del data[H.MODEL_GROSS_LOSS]

    schema_without = {k: v for k, v in _minimal_fanout_schema().items() if k != H.MODEL_GROSS_LOSS}
    bad = tmp_path / "HiscoAIR_202601_main.parquet"
    pl.DataFrame(data, schema=pl.Schema(schema_without)).write_parquet(bad)

    with pytest.raises(SchemaError, match=H.MODEL_GROSS_LOSS):
        push_parquet_to_sql(bad, conn_str="mssql+pyodbc://unused/unused")


def test_push_rejects_parquet_with_wrong_dtype(tmp_path: Path) -> None:
    """A parquet whose column dtype mismatches HISCO_FANOUT raises SchemaError."""
    from rollup.io.sql_push import push_parquet_to_sql

    # ModelGrossLoss should be Float64 but we write it as Int64
    data = _minimal_fanout_df()
    data[H.MODEL_GROSS_LOSS] = [100]   # int instead of float

    wrong_schema = dict(_minimal_fanout_schema())
    wrong_schema[H.MODEL_GROSS_LOSS] = pl.Int64

    bad = tmp_path / "HiscoAIR_202601_main.parquet"
    pl.DataFrame(data, schema=pl.Schema(wrong_schema)).write_parquet(bad)

    with pytest.raises(SchemaError, match=H.MODEL_GROSS_LOSS):
        push_parquet_to_sql(bad, conn_str="mssql+pyodbc://unused/unused")


def test_push_raises_error_without_engine_or_conn_str(tmp_path: Path) -> None:
    """Supplying neither engine nor conn_str raises ValueError before reading the parquet."""
    from rollup.io.sql_push import push_parquet_to_sql

    # Use a non-existent path — the error must come from the argument check, not I/O.
    with pytest.raises(ValueError, match="engine"):
        push_parquet_to_sql(tmp_path / "nonexistent.parquet")
