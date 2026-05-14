"""Schema validation for LazyFrames / DataFrames at stage boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import polars as pl


class SchemaError(AssertionError):
    """Frame schema does not match the expected pl.Schema."""


@dataclass(frozen=True)
class ColumnDiff:
    column: str
    kind:   Literal["missing", "unexpected", "wrong_dtype"]
    detail: str   # expected dtype for missing; actual dtype for unexpected; "Float64→Int64" for wrong_dtype


def column_diff(actual: pl.Schema, expected: pl.Schema) -> list[ColumnDiff]:
    """Return ColumnDiff entries: missing first, then wrong_dtype, then unexpected.
    Each group sorted alphabetically by column name."""
    actual_cols   = set(actual.names())
    expected_cols = set(expected.names())
    missing    = sorted(str(c) for c in expected_cols - actual_cols)
    unexpected = sorted(str(c) for c in actual_cols - expected_cols)
    wrong      = sorted(str(c) for c in (actual_cols & expected_cols) if actual[c] != expected[c])
    out: list[ColumnDiff] = []
    for c in missing:
        out.append(ColumnDiff(c, "missing", str(expected[c])))
    for c in wrong:
        out.append(ColumnDiff(c, "wrong_dtype", f"{expected[c]}→{actual[c]}"))
    for c in unexpected:
        out.append(ColumnDiff(c, "unexpected", str(actual[c])))
    return out


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


def validate_column_in_enum(
    frame: pl.DataFrame | pl.LazyFrame,
    column: str,
    allowed: set[str],
    *,
    name: str,
) -> None:
    """Assert every value in `column` is a member of `allowed`.

    Polars `pl.String` cannot encode a closed value set at the schema level, so
    seeds that carry a domain enum (e.g. `base_model ∈ {verisk, risklink}`) get
    a runtime check at load. A typo in the CSV would otherwise silently mean a
    different vendor's AAL becomes the uplift denominator — invisible at every
    layer until the output numbers are wrong.
    """
    materialized = frame.collect() if isinstance(frame, pl.LazyFrame) else frame
    actual = set(materialized[column].drop_nulls().unique().to_list())
    invalid = sorted(actual - allowed)
    if invalid:
        raise SchemaError(
            f"[{name}] column {column!r} has values outside the allowed set "
            f"{sorted(allowed)}: {invalid}"
        )
