"""Confirm the enum-as-column-name pattern actually works with polars."""

from __future__ import annotations

import polars as pl
import pytest

from rollup.config import VendorName
from rollup.schemas import frames as F
from rollup.schemas.columns import NormalizedYltCol as Y
from rollup.schemas.columns import RawRisklinkYltCol as RLK
from rollup.validate import SchemaError, validate_schema


# -----------------------------------------------------------------------------
# Enum-as-string identity
# -----------------------------------------------------------------------------

def test_strenum_member_is_string():
    assert RLK.LOSS == "loss"
    assert isinstance(RLK.LOSS, str)


def test_pl_col_accepts_strenum_member():
    """`pl.col(C.FOO)` and `pl.col("foo")` produce equivalent expressions."""
    df = pl.DataFrame({"loss": [1.0, 2.0, 3.0]})
    left = df.select(pl.col(RLK.LOSS)).to_series().to_list()
    right = df.select(pl.col("loss")).to_series().to_list()
    assert left == right == [1.0, 2.0, 3.0]


def test_pl_schema_accepts_strenum_keys():
    """Schemas can be declared directly with enum members as keys."""
    schema = pl.Schema({RLK.LOSS: pl.Float64, RLK.YEAR_ID: pl.Int64})
    assert schema["loss"] == pl.Float64
    assert schema["yearid"] == pl.Int64


def test_rename_with_strenum():
    """`df.rename({C.OLD: C.NEW})` works because StrEnums ARE strings."""
    df = pl.DataFrame({"loss": [1.0]})
    renamed = df.rename({RLK.LOSS: Y.LOSS})  # "loss" -> "loss" here, same name
    assert renamed.columns == ["loss"]


# -----------------------------------------------------------------------------
# validate_schema behavior
# -----------------------------------------------------------------------------

def _normalized_ylt_frame(**overrides) -> pl.DataFrame:
    base = {
        Y.VENDOR: [VendorName.RISKLINK],
        Y.LOB_ID: [1],
        Y.MODELLED_LOB: ["x"],
        Y.ROLLUP_LOB: ["x"],
        Y.LOB_TYPE: ["prop"],
        Y.CDS_CAT_CLASS_NAME: ["x"],
        Y.OFFICE: ["UK"],
        Y.LOB_CLASS: ["HH"],
        Y.REGION_PERIL_ID: [206],
        Y.MODELLED_REGION_PERIL: ["EU_WS"],
        Y.PERIL_NAME: ["Europe Winter Storm"],
        Y.REGION: ["EU"],
        Y.PERIL_FAMILY: ["WS"],
        Y.CURRENCY: ["GBP"],
        Y.MODEL_CODE: [0],
        Y.YEAR_ID: [1],
        Y.EVENT_ID: [1],
        Y.LOSS: [1.0],
    }
    base.update(overrides)
    return pl.DataFrame(base, schema=F.NORMALIZED_YLT)


def test_validate_schema_happy_path():
    validate_schema(_normalized_ylt_frame(), F.NORMALIZED_YLT, name="ok")


def test_validate_schema_rejects_missing_column():
    df = _normalized_ylt_frame().drop(Y.LOSS)
    with pytest.raises(SchemaError, match="missing columns"):
        validate_schema(df, F.NORMALIZED_YLT)


def test_validate_schema_rejects_dtype_mismatch():
    df = _normalized_ylt_frame().with_columns(pl.col(Y.LOSS).cast(pl.Float32))
    with pytest.raises(SchemaError, match="dtype mismatches"):
        validate_schema(df, F.NORMALIZED_YLT)


def test_validate_schema_rejects_extra_column_when_strict():
    df = _normalized_ylt_frame().with_columns(pl.lit(1).alias("bonus"))
    with pytest.raises(SchemaError, match="unexpected columns"):
        validate_schema(df, F.NORMALIZED_YLT, strict=True)


def test_validate_schema_allows_extra_column_when_not_strict():
    df = _normalized_ylt_frame().with_columns(pl.lit(1).alias("bonus"))
    validate_schema(df, F.NORMALIZED_YLT, strict=False)


def test_validate_schema_works_on_lazyframe():
    validate_schema(_normalized_ylt_frame().lazy(), F.NORMALIZED_YLT, name="lazy")
