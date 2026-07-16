from __future__ import annotations

from collections.abc import Mapping, Sequence

import polars as pl


DATE_DTYPES = {pl.Date, pl.Datetime}


def collect_lazy_schema(model: str, input_name: str, frame: pl.LazyFrame) -> pl.Schema:
    try:
        return frame.collect_schema()
    except Exception as exc:  # pragma: no cover - preserves original Polars context
        raise ValueError(
            f"{model}: could not resolve schema for input '{input_name}': {exc}"
        ) from exc


def require_columns(
    model: str,
    input_name: str,
    schema: Mapping[str, pl.DataType],
    columns: Sequence[str],
) -> None:
    missing = [column for column in columns if column not in schema]
    if missing:
        available = ", ".join(str(column) for column in schema.keys())
        raise ValueError(
            f"{model}: input '{input_name}' missing required columns {missing}; available columns: [{available}]"
        )


def require_dtype_family(
    model: str,
    input_name: str,
    schema: Mapping[str, pl.DataType],
    column: str,
    family: str,
) -> None:
    require_columns(model, input_name, schema, [column])
    dtype = schema[column]
    if family == "numeric":
        valid = dtype.is_numeric()
    elif family == "integer":
        valid = dtype.is_integer()
    elif family == "date_like":
        valid = dtype.base_type() in DATE_DTYPES
    else:  # pragma: no cover - developer error
        raise ValueError(f"unknown dtype family: {family}")
    if not valid:
        raise ValueError(
            f"{model}: input '{input_name}' column '{column}' has incompatible dtype {dtype}; expected {family}"
        )


def require_join_key_compatible(
    model: str,
    left_name: str,
    left_schema: Mapping[str, pl.DataType],
    right_name: str,
    right_schema: Mapping[str, pl.DataType],
    keys: Sequence[str],
) -> None:
    require_columns(model, left_name, left_schema, keys)
    require_columns(model, right_name, right_schema, keys)
    incompatible = []
    for key in keys:
        left_dtype = left_schema[key]
        right_dtype = right_schema[key]
        if left_dtype == right_dtype:
            continue
        if left_dtype.is_numeric() and right_dtype.is_numeric():
            continue
        incompatible.append(f"{key}: {left_dtype} != {right_dtype}")
    if incompatible:
        raise ValueError(
            f"{model}: join key dtype mismatch between '{left_name}' and '{right_name}': {incompatible}"
        )


def validate_output(model: str, frame: pl.LazyFrame) -> None:
    collect_lazy_schema(model, "output", frame)


def validate_mapping_key(
    model: str,
    mapping_name: str,
    mapping: Mapping[str, pl.LazyFrame],
    key: str,
) -> None:
    if key not in mapping:
        available = ", ".join(sorted(mapping.keys()))
        raise ValueError(
            f"{model}: input '{mapping_name}' missing required key '{key}'; available keys: [{available}]"
        )
