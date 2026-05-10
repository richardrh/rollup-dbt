"""Hypothesis strategy library for the rollup pipeline.

Public API
----------
lazyframe_from_schema(schema, min_rows, max_rows)
    Given any ``pl.Schema``, generate a valid ``pl.LazyFrame`` matching it.

realistic_loss_strategy()
    Positive ``Float64``, occasional zero.  No NaN/inf.

pathological_loss_strategy()
    Full ``Float64`` range including NaN, inf, and negative values.

_dtype_strategy(dtype)
    Map a single polars dtype to a Hypothesis strategy.  Used internally
    by ``lazyframe_from_schema``; exported for callers that need to build
    column-level strategies directly.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl
from hypothesis import strategies as st
from hypothesis.strategies import SearchStrategy


# ---------------------------------------------------------------------------
# Public numeric strategies
# ---------------------------------------------------------------------------

def realistic_loss_strategy() -> SearchStrategy[float]:
    """Positive ``Float64``, occasional zero.  No NaN / inf."""
    return st.one_of(
        st.just(0.0),
        st.floats(
            min_value=0.0,
            max_value=1e15,
            allow_nan=False,
            allow_infinity=False,
            width=64,
        ),
    )


def pathological_loss_strategy() -> SearchStrategy[float | None]:
    """Full ``Float64`` range including NaN, inf, and negative values.

    Use this when you want to stress-test guards against numeric edge
    cases (divide-by-zero, inf propagation, negative losses, etc.).
    """
    return st.one_of(
        st.floats(allow_nan=True, allow_infinity=True, width=64),
        st.just(float("-inf")),
        st.just(float("inf")),
        st.just(float("nan")),
        st.just(None),  # null / missing
    )


# ---------------------------------------------------------------------------
# Internal: per-dtype strategies
# ---------------------------------------------------------------------------

def _dtype_strategy(dtype: pl.DataType) -> SearchStrategy[Any]:
    """Map a single polars dtype to a Hypothesis strategy.

    Floats default to *no* NaN/inf so that generated frames are valid
    for downstream arithmetic.  Use ``pathological_loss_strategy`` when
    you specifically want numeric edge cases.

    ``pl.Date`` values are restricted to 1970-01-01 … 2050-12-31 to
    avoid polars representation edge cases with dates outside that range.
    """
    # --- integer types ---
    if dtype == pl.Int64:
        return st.integers(min_value=-(2**31), max_value=2**31 - 1)
    if dtype == pl.Int32:
        return st.integers(min_value=-(2**31), max_value=2**31 - 1)
    if dtype == pl.Int16:
        return st.integers(min_value=-(2**15), max_value=2**15 - 1)
    if dtype == pl.Int8:
        return st.integers(min_value=-(2**7), max_value=2**7 - 1)
    if dtype == pl.UInt64:
        return st.integers(min_value=0, max_value=2**31 - 1)
    if dtype == pl.UInt32:
        return st.integers(min_value=0, max_value=2**31 - 1)
    if dtype == pl.UInt16:
        return st.integers(min_value=0, max_value=2**16 - 1)
    if dtype == pl.UInt8:
        return st.integers(min_value=0, max_value=2**8 - 1)
    # --- floating-point types ---
    if dtype in (pl.Float64, pl.Float32):
        return st.floats(
            allow_nan=False,
            allow_infinity=False,
            width=64,
        )
    # --- text ---
    if dtype == pl.String:
        return st.text(min_size=0, max_size=200)
    # --- boolean ---
    if dtype == pl.Boolean:
        return st.booleans()
    # --- date ---
    if dtype == pl.Date:
        return st.dates(
            min_value=date(1970, 1, 1),
            max_value=date(2050, 12, 31),
        )
    # --- datetime (no timezone) ---
    if dtype == pl.Datetime:
        return st.datetimes(
            min_value=__import__("datetime").datetime(1970, 1, 1),
            max_value=__import__("datetime").datetime(2050, 12, 31),
        )
    # --- fallback: treat as nullable text to avoid hard failures on unknown dtypes ---
    return st.one_of(st.none(), st.text(min_size=0, max_size=50))


# ---------------------------------------------------------------------------
# Core public strategy
# ---------------------------------------------------------------------------

@st.composite
def lazyframe_from_schema(
    draw: st.DrawFn,
    schema: pl.Schema,
    min_rows: int = 0,
    max_rows: int = 100,
) -> pl.LazyFrame:
    """Generate a valid ``pl.LazyFrame`` matching ``schema``.

    Parameters
    ----------
    schema:
        The ``pl.Schema`` the generated frame must satisfy exactly.
    min_rows:
        Minimum number of rows in the generated frame.
    max_rows:
        Maximum number of rows in the generated frame.

    Returns
    -------
    pl.LazyFrame
        A lazy frame whose ``collect_schema()`` equals ``schema`` and
        whose data is drawn from per-dtype strategies.
    """
    n_rows: int = draw(st.integers(min_value=min_rows, max_value=max_rows))

    # Build one series per column.
    series: dict[str, pl.Series] = {}
    for col_name, dtype in schema.items():
        col_strategy = _dtype_strategy(dtype)
        values: list[Any] = draw(st.lists(col_strategy, min_size=n_rows, max_size=n_rows))
        # ``pl.Series`` coerces Python scalars to the correct polars dtype.
        # We cast explicitly to guarantee the schema matches — some dtypes
        # (e.g. pl.Date) require this because polars stores them as int32
        # under the hood.
        series[col_name] = pl.Series(name=col_name, values=values, dtype=dtype)

    return pl.LazyFrame(series, schema=schema)
