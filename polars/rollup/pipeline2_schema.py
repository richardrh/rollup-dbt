"""Minimal YAML-backed schema helpers for the isolated pipeline2 path."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import yaml


DEFAULT_SCHEMA_PATH = Path(__file__).with_name("pipeline2_schema.yaml")

_DTYPES: dict[str, pl.DataType] = {
    "bool": pl.Boolean,
    "boolean": pl.Boolean,
    "date": pl.Date,
    "datetime": pl.Datetime,
    "float32": pl.Float32,
    "float64": pl.Float64,
    "int32": pl.Int32,
    "int64": pl.Int64,
    "string": pl.String,
    "str": pl.String,
    "uint32": pl.UInt32,
    "uint64": pl.UInt64,
}


class Pipeline2SchemaError(ValueError):
    """Raised when pipeline2 YAML contracts or frames are invalid."""


@dataclass(frozen=True)
class ColumnSpec:
    """One declarative column contract from ``pipeline2_schema.yaml``."""

    name: str
    dtype_name: str
    dtype: pl.DataType
    required: bool
    description: str


@dataclass(frozen=True)
class DatasetSpec:
    """One dataset contract from ``pipeline2_schema.yaml``."""

    name: str
    role: str
    format: str
    columns: tuple[ColumnSpec, ...]
    description: str
    required: bool = True
    path: str | None = None
    glob: str | None = None
    status: str | None = None

    @property
    def pl_schema(self) -> pl.Schema:
        """Polars schema containing every declared column."""

        return pl.Schema({column.name: column.dtype for column in self.columns})

    @property
    def required_columns(self) -> tuple[ColumnSpec, ...]:
        """Columns that must be present on the dataframe."""

        return tuple(column for column in self.columns if column.required)


@dataclass(frozen=True)
class Pipeline2Schema:
    """Loaded pipeline2 YAML schema."""

    version: int
    description: str
    datasets: dict[str, DatasetSpec]

    def dataset(self, name: str) -> DatasetSpec:
        """Return a named dataset spec or raise a schema error."""

        try:
            return self.datasets[name]
        except KeyError as exc:
            raise Pipeline2SchemaError(f"unknown pipeline2 dataset: {name}") from exc


def polars_dtype(dtype_name: str) -> pl.DataType:
    """Convert a YAML dtype name into a Polars dtype."""

    try:
        return _DTYPES[dtype_name.lower()]
    except KeyError as exc:
        raise Pipeline2SchemaError(f"unsupported pipeline2 dtype: {dtype_name}") from exc


def load_pipeline2_schema(path: Path | str = DEFAULT_SCHEMA_PATH) -> Pipeline2Schema:
    """Load and minimally validate the pipeline2 YAML schema file."""

    schema_path = Path(path)
    raw = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise Pipeline2SchemaError("pipeline2 schema root must be a mapping")


    version = raw.get("version")
    description = raw.get("description")
    raw_datasets = raw.get("datasets")
    if not isinstance(version, int):
        raise Pipeline2SchemaError("pipeline2 schema version must be an integer")
    if not isinstance(description, str) or not description.strip():
        raise Pipeline2SchemaError("pipeline2 schema description is required")
    if not isinstance(raw_datasets, dict) or not raw_datasets:
        raise Pipeline2SchemaError("pipeline2 schema datasets must be a non-empty mapping")

    datasets = {
        name: _parse_dataset(name, raw_spec)
        for name, raw_spec in raw_datasets.items()
    }
    return Pipeline2Schema(version=version, description=description, datasets=datasets)


def validate_columns(
    frame: pl.DataFrame | pl.LazyFrame,
    spec: DatasetSpec,
    *,
    strict: bool = True,
) -> None:
    """Validate dataframe columns against a pipeline2 dataset spec."""

    schema = frame.collect_schema() if isinstance(frame, pl.LazyFrame) else frame.schema
    actual_columns = set(schema)
    required_columns = {column.name for column in spec.required_columns}
    declared_columns = {column.name for column in spec.columns}

    missing = sorted(required_columns - actual_columns)
    if missing:
        raise Pipeline2SchemaError(f"{spec.name}: missing columns: {missing}")

    if strict:
        extra = sorted(actual_columns - declared_columns)
        if extra:
            raise Pipeline2SchemaError(f"{spec.name}: unexpected columns: {extra}")

    mismatches = []
    for column in spec.columns:
        if column.name in schema and schema[column.name] != column.dtype:
            mismatches.append(f"{column.name}: expected {column.dtype}, got {schema[column.name]}")
    if mismatches:
        raise Pipeline2SchemaError(f"{spec.name}: dtype mismatches: {mismatches}")


def _parse_dataset(name: str, raw_spec: Any) -> DatasetSpec:
    if not isinstance(name, str) or not name:
        raise Pipeline2SchemaError("dataset names must be non-empty strings")
    if not isinstance(raw_spec, dict):
        raise Pipeline2SchemaError(f"{name}: dataset spec must be a mapping")

    columns = raw_spec.get("columns")
    if not isinstance(columns, list) or not columns:
        raise Pipeline2SchemaError(f"{name}: columns must be a non-empty list")

    role = _required_string(raw_spec, "role", name)
    data_format = _required_string(raw_spec, "format", name)
    description = _required_string(raw_spec, "description", name)

    return DatasetSpec(
        name=name,
        role=role,
        format=data_format,
        description=description,
        required=bool(raw_spec.get("required", True)),
        path=_optional_string(raw_spec, "path", name),
        glob=_optional_string(raw_spec, "glob", name),
        status=_optional_string(raw_spec, "status", name),
        columns=tuple(_parse_column(name, column) for column in columns),
    )


def _parse_column(dataset_name: str, raw_column: Any) -> ColumnSpec:
    if not isinstance(raw_column, dict):
        raise Pipeline2SchemaError(f"{dataset_name}: column specs must be mappings")

    name = _required_string(raw_column, "name", dataset_name)
    dtype_name = _required_string(raw_column, "dtype", dataset_name)
    description = _required_string(raw_column, "description", dataset_name)

    if "required" not in raw_column or not isinstance(raw_column["required"], bool):
        raise Pipeline2SchemaError(f"{dataset_name}.{name}: required must be a boolean")

    return ColumnSpec(
        name=name,
        dtype_name=dtype_name,
        dtype=polars_dtype(dtype_name),
        required=raw_column["required"],
        description=description,
    )


def _required_string(raw: dict[str, Any], key: str, context: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise Pipeline2SchemaError(f"{context}: {key} is required")
    return value


def _optional_string(raw: dict[str, Any], key: str, context: str) -> str | None:
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise Pipeline2SchemaError(f"{context}: {key} must be a string when provided")
    return value
