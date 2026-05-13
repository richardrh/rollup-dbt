"""Property-based schema roundtrip tests.

For every static ``pl.Schema`` in ``rollup.schemas.frames``, three
properties are checked:

1. **Generate-and-validate** — ``validate_schema`` does not raise on a
   generated frame.
2. **Schema match** — ``generated_lf.collect_schema() == schema``.
3. **Parquet round-trip** — write to ``tmp_path``, read back; schema is
   preserved.

All tests are marked ``@pytest.mark.fuzz`` and skipped unless
``--run-fuzz`` is passed.  ``ALL_FACTORS`` is skipped because it gains
dynamic ``f_{yyyymm}`` forecast-factor columns at runtime — those are
fuzzed in Phase 2.
"""

from __future__ import annotations

import io
from typing import Any

import polars as pl
import pytest
from hypothesis import given, settings

import rollup.schemas.frames as F
from rollup.validate import validate_schema

from .strategies import lazyframe_from_schema


# ---------------------------------------------------------------------------
# Schema registry — all static schemas eligible for roundtrip testing.
# ALL_FACTORS is excluded: it has runtime-injected f_{yyyymm} columns.
# ---------------------------------------------------------------------------

_STATIC_SCHEMAS: list[tuple[str, pl.Schema]] = [
    ("RAW_RISKLINK_YLT",       F.RAW_RISKLINK_YLT),
    ("RAW_VERISK_YLT",         F.RAW_VERISK_YLT),
    ("PERILS",                 F.PERILS),
    ("ANALYSES",               F.ANALYSES),
    ("VALID_ANALYSES",         F.VALID_ANALYSES),
    ("BLENDING_WEIGHTS",       F.BLENDING_WEIGHTS),
    ("REF_LOBS",               F.REF_LOBS),
    ("REF_FORECAST_FACTORS",   F.REF_FORECAST_FACTORS),
    ("REF_FX_RATES",           F.REF_FX_RATES),
    ("REF_EUWS_RATE_FACTORS",  F.REF_EUWS_RATE_FACTORS),
    ("REF_EUWS_RANK_OVERRIDES", F.REF_EUWS_RANK_OVERRIDES),
    ("REF_AIR_EVENTS",         F.REF_AIR_EVENTS),
    ("REF_RISKLINK_EVENTS",    F.REF_RISKLINK_EVENTS),
    ("REF_FINEART_ADJ",        F.REF_FINEART_ADJ),
    ("STG_RISKLINK_EP",        F.STG_RISKLINK_EP),
    ("STG_VERISK_EP",          F.STG_VERISK_EP),
    ("NORMALIZED_YLT",         F.NORMALIZED_YLT),
    ("EP_CURVE",               F.EP_CURVE),
    ("METRICS",                F.METRICS),
    ("HISCO_FANOUT",           F.HISCO_FANOUT),
    # ALL_FACTORS intentionally excluded — see module docstring.
]

_SCHEMA_IDS: list[str] = [name for name, _ in _STATIC_SCHEMAS]


# ---------------------------------------------------------------------------
# Property 1: generate-and-validate
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@pytest.mark.parametrize("schema_name,schema", _STATIC_SCHEMAS, ids=_SCHEMA_IDS)
def test_generated_frame_passes_validate_schema(
    schema_name: str,
    schema: pl.Schema,
) -> None:
    """``validate_schema`` does not raise for any generated frame."""

    @given(lf=lazyframe_from_schema(schema))  # type: ignore[arg-type]
    def _inner(lf: pl.LazyFrame) -> None:
        validate_schema(lf, schema, name=schema_name)

    _inner()


# ---------------------------------------------------------------------------
# Property 2: schema identity
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@pytest.mark.parametrize("schema_name,schema", _STATIC_SCHEMAS, ids=_SCHEMA_IDS)
def test_generated_frame_collect_schema_matches(
    schema_name: str,
    schema: pl.Schema,
) -> None:
    """``collect_schema()`` of a generated frame equals the source schema."""

    @given(lf=lazyframe_from_schema(schema))  # type: ignore[arg-type]
    def _inner(lf: pl.LazyFrame) -> None:
        assert lf.collect_schema() == schema, (
            f"[{schema_name}] collect_schema() mismatch: "
            f"{lf.collect_schema()} != {schema}"
        )

    _inner()


# ---------------------------------------------------------------------------
# Property 3: parquet round-trip
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@pytest.mark.parametrize("schema_name,schema", _STATIC_SCHEMAS, ids=_SCHEMA_IDS)
def test_generated_frame_parquet_roundtrip(
    schema_name: str,
    schema: pl.Schema,
    tmp_path: Any,
) -> None:
    """Schema is preserved after writing to Parquet and reading back."""

    @given(lf=lazyframe_from_schema(schema))  # type: ignore[arg-type]
    def _inner(lf: pl.LazyFrame) -> None:
        buf = io.BytesIO()
        lf.collect().write_parquet(buf)
        buf.seek(0)
        recovered = pl.read_parquet(buf)
        assert recovered.schema == schema, (
            f"[{schema_name}] parquet round-trip schema mismatch: "
            f"{recovered.schema} != {schema}"
        )

    _inner()
