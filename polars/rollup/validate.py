"""Schema validation for LazyFrames / DataFrames at stage boundaries."""

from __future__ import annotations

import polars as pl


class SchemaError(AssertionError):
    """Frame schema does not match the expected pl.Schema."""


def validate_schema(
    frame: pl.DataFrame | pl.LazyFrame,
    expected: pl.Schema,
    *,
    name: str = "frame",
    strict: bool = True,
) -> None:
    """Assert `frame`'s schema matches `expected`.

    strict=True  → column set AND dtypes must match exactly.
    strict=False → every expected column must exist with the expected dtype;
                   extra columns are allowed.
    """
    actual = frame.collect_schema() if isinstance(frame, pl.LazyFrame) else frame.schema
    actual_names = set(actual.names())

    missing = [c for c in expected.names() if c not in actual_names]
    if missing:
        raise SchemaError(f"[{name}] missing columns: {missing}")

    mismatches = [
        f"  {c}: got {actual[c]}, want {dt}"
        for c, dt in expected.items() if actual[c] != dt
    ]
    if mismatches:
        raise SchemaError(f"[{name}] dtype mismatches:\n" + "\n".join(mismatches))

    if strict:
        extras = [c for c in actual.names() if c not in expected.names()]
        if extras:
            raise SchemaError(f"[{name}] unexpected columns: {extras}")
