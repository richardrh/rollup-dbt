from __future__ import annotations

import polars as pl
import pytest

from rollup.pipeline2_schema import (
    DEFAULT_SCHEMA_PATHS,
    Pipeline2SchemaError,
    load_pipeline2_schema,
    polars_dtype,
    validate_columns,
)


def test_pipeline2_schema_loads_and_maps_dtypes() -> None:
    schema = load_pipeline2_schema()
    spec = schema.dataset("mart_loss_summary")

    assert schema.version == 1
    assert tuple(path.name for path in DEFAULT_SCHEMA_PATHS) == ("schema.yaml", "schema.yaml", "schema.yaml", "schema.yaml")
    assert polars_dtype("string") == pl.String
    assert polars_dtype("uint32") == pl.UInt32
    assert spec.pl_schema == pl.Schema({
        "vendor": pl.String,
        "analysis_id": pl.String,
        "total_loss": pl.Float64,
        "event_count": pl.UInt32,
    })


def test_pipeline2_validation_accepts_optional_missing_columns() -> None:
    schema = load_pipeline2_schema()
    spec = schema.dataset("analyses")
    frame = pl.DataFrame(
        {
            "vendor": ["verisk"],
            "analysis_id": ["100"],
            "modelled_label": ["EUWS"],
            "peril_id": [1],
        },
        schema={
            "vendor": pl.String,
            "analysis_id": pl.String,
            "modelled_label": pl.String,
            "peril_id": pl.Int64,
        },
    )

    validate_columns(frame, spec)


def test_pipeline2_validation_rejects_missing_required_columns() -> None:
    schema = load_pipeline2_schema()
    spec = schema.dataset("mart_loss_summary")
    frame = pl.DataFrame(
        {"vendor": ["verisk"], "analysis_id": ["100"], "event_count": [1]},
        schema={"vendor": pl.String, "analysis_id": pl.String, "event_count": pl.UInt32},
    )

    with pytest.raises(Pipeline2SchemaError, match="missing columns"):
        validate_columns(frame, spec)


def test_pipeline2_validation_rejects_extra_columns_when_strict() -> None:
    schema = load_pipeline2_schema()
    spec = schema.dataset("mart_loss_summary")
    frame = pl.DataFrame(
        {
            "vendor": ["verisk"],
            "analysis_id": ["100"],
            "total_loss": [10.0],
            "event_count": [1],
            "debug": [True],
        },
        schema={
            "vendor": pl.String,
            "analysis_id": pl.String,
            "total_loss": pl.Float64,
            "event_count": pl.UInt32,
            "debug": pl.Boolean,
        },
    )

    with pytest.raises(Pipeline2SchemaError, match="unexpected columns"):
        validate_columns(frame, spec, strict=True)


def test_pipeline2_validation_rejects_dtype_mismatches() -> None:
    schema = load_pipeline2_schema()
    spec = schema.dataset("mart_loss_summary")
    frame = pl.DataFrame(
        {
            "vendor": ["verisk"],
            "analysis_id": ["100"],
            "total_loss": [10.0],
            "event_count": [1],
        },
        schema={
            "vendor": pl.String,
            "analysis_id": pl.String,
            "total_loss": pl.Float64,
            "event_count": pl.Int64,
        },
    )

    with pytest.raises(Pipeline2SchemaError, match="dtype mismatches"):
        validate_columns(frame.lazy(), spec)
