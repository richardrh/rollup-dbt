from __future__ import annotations

from collections.abc import Mapping

import polars as pl


class SchemaGuardError(ValueError):
    """Raised when a runtime Polars schema guard fails."""


def require_columns(
    frame_or_schema: pl.DataFrame | pl.LazyFrame | Mapping[str, pl.DataType],
    schema: pl.Schema,
    *,
    allow_extra: bool = True,
    check_dtypes: bool = True,
) -> None:
    actual = _schema(frame_or_schema)
    missing = [name for name in schema if name not in actual]
    extra = [name for name in actual if name not in schema]
    dtype_mismatches = [
        f"{name}: expected {schema[name]}, got {actual[name]}"
        for name in schema
        if name in actual and check_dtypes and actual[name] != schema[name]
    ]

    errors: list[str] = []
    if missing:
        errors.append(f"missing columns: {missing}")
    if extra and not allow_extra:
        errors.append(f"unexpected columns: {extra}")
    if dtype_mismatches:
        errors.append(f"dtype mismatches: {dtype_mismatches}")
    if errors:
        raise SchemaGuardError("; ".join(errors))


def _schema(
    frame_or_schema: pl.DataFrame | pl.LazyFrame | Mapping[str, pl.DataType],
) -> Mapping[str, pl.DataType]:
    if isinstance(frame_or_schema, pl.LazyFrame):
        return frame_or_schema.collect_schema()
    if isinstance(frame_or_schema, pl.DataFrame):
        return frame_or_schema.schema
    return frame_or_schema
