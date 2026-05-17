"""Orchestration for the isolated, schema-driven pipeline2 Polars flow."""

from __future__ import annotations

from dataclasses import dataclass
from glob import glob
from pathlib import Path

import polars as pl

from rollup.intermediate.pipeline2 import select_losses
from rollup.marts.pipeline2 import summarize_losses
from rollup.pipeline2_schema import (
    DatasetSpec,
    Pipeline2Schema,
    Pipeline2SchemaError,
    load_pipeline2_schema,
    validate_columns,
)
from rollup.staging.pipeline2 import build_normalized_ylt, stage_selected_analyses


@dataclass(frozen=True)
class Pipeline2Sources:
    """Source LazyFrames after YAML-backed column validation."""

    selected_analyses: pl.LazyFrame
    raw_ylt: tuple[pl.LazyFrame, ...]


@dataclass(frozen=True)
class Pipeline2Staging:
    """Staging models for the initial pipeline2 flow."""

    selected_analyses: pl.LazyFrame
    normalized_ylt: pl.LazyFrame


@dataclass(frozen=True)
class Pipeline2Intermediate:
    """Intermediate models for the initial pipeline2 flow."""

    selected_losses: pl.LazyFrame


@dataclass(frozen=True)
class Pipeline2Marts:
    """Mart models for the initial pipeline2 flow."""

    loss_summary: pl.LazyFrame


@dataclass(frozen=True)
class Pipeline2Models:
    """All models produced by the small linear pipeline2 DAG."""

    sources: Pipeline2Sources
    staging: Pipeline2Staging
    intermediate: Pipeline2Intermediate
    marts: Pipeline2Marts


def load_dataset(spec: DatasetSpec, *, root: Path | str = Path.cwd()) -> pl.LazyFrame:
    """Scan a YAML-declared dataset and validate its columns."""

    root_path = Path(root)
    if spec.path is not None:
        frame = _scan_path(root_path / spec.path, spec)
    elif spec.glob is not None:
        frame = _scan_glob(root_path, spec)
    else:
        raise Pipeline2SchemaError(f"{spec.name}: expected path or glob")

    validate_columns(frame, spec, strict=True)
    return frame


def selected_analyses_spec(schema: Pipeline2Schema, *, root: Path | str = Path.cwd()) -> DatasetSpec:
    """Prefer first-class selected_analyses, with valid_analyses as legacy fallback."""

    root_path = Path(root)
    selected = schema.dataset("selected_analyses")
    if selected.path is not None and (root_path / selected.path).exists():
        return selected

    fallback = schema.dataset("valid_analyses")
    if fallback.path is not None and (root_path / fallback.path).exists():
        return fallback

    raise Pipeline2SchemaError(
        "selected_analyses is required; valid_analyses may be used only as a legacy fallback"
    )


def build_sources(
    *,
    root: Path | str = Path.cwd(),
    schema: Pipeline2Schema | None = None,
) -> Pipeline2Sources:
    """Load and validate the source frames needed by the initial pipeline2 flow."""

    loaded_schema = schema or load_pipeline2_schema()
    selected_spec = selected_analyses_spec(loaded_schema, root=root)
    selected = load_dataset(selected_spec, root=root)
    validate_columns(selected, loaded_schema.dataset("stg_selected_analyses"), strict=True)

    raw_ylt = (
        load_dataset(loaded_schema.dataset("raw_risklink_ylt"), root=root),
        load_dataset(loaded_schema.dataset("raw_verisk_ylt"), root=root),
    )
    return Pipeline2Sources(selected_analyses=selected, raw_ylt=raw_ylt)


def preflight_pipeline2_boundary(sources: Pipeline2Sources, schema: Pipeline2Schema) -> None:
    """Validate source contracts before any pipeline2 transformations run."""

    validate_columns(sources.selected_analyses, schema.dataset("stg_selected_analyses"), strict=True)
    validate_columns(sources.raw_ylt[0], schema.dataset("raw_risklink_ylt"), strict=True)
    validate_columns(sources.raw_ylt[1], schema.dataset("raw_verisk_ylt"), strict=True)


def build_staging(sources: Pipeline2Sources, schema: Pipeline2Schema) -> Pipeline2Staging:
    """Build staging models from already-validated source frames."""

    risklink, verisk = sources.raw_ylt
    selected_analyses = stage_selected_analyses(sources.selected_analyses)
    normalized_ylt = build_normalized_ylt(risklink, verisk)
    validate_columns(selected_analyses, schema.dataset("stg_selected_analyses"), strict=True)
    validate_columns(normalized_ylt, schema.dataset("stg_normalized_ylt"), strict=True)
    return Pipeline2Staging(selected_analyses=selected_analyses, normalized_ylt=normalized_ylt)


def build_intermediate(
    sources: Pipeline2Sources,
    staging: Pipeline2Staging,
    schema: Pipeline2Schema,
) -> Pipeline2Intermediate:
    """Filter staged losses to the selected analysis allow-list."""

    selected_losses = select_losses(staging.normalized_ylt, staging.selected_analyses)
    validate_columns(selected_losses, schema.dataset("int_selected_losses"), strict=True)
    return Pipeline2Intermediate(selected_losses=selected_losses)


def build_marts(intermediate: Pipeline2Intermediate, schema: Pipeline2Schema) -> Pipeline2Marts:
    """Build the initial pipeline2 mart output."""

    loss_summary = summarize_losses(intermediate.selected_losses)
    validate_columns(loss_summary, schema.dataset("mart_loss_summary"), strict=True)
    return Pipeline2Marts(loss_summary=loss_summary)


def build_pipeline2(
    *,
    root: Path | str = Path.cwd(),
    schema: Pipeline2Schema | None = None,
) -> Pipeline2Models:
    """Build the full initial pipeline2 DAG without collecting it."""

    loaded_schema = schema or load_pipeline2_schema()
    sources = build_sources(root=root, schema=loaded_schema)
    preflight_pipeline2_boundary(sources, loaded_schema)
    staging = build_staging(sources, loaded_schema)
    intermediate = build_intermediate(sources, staging, loaded_schema)
    marts = build_marts(intermediate, loaded_schema)
    return Pipeline2Models(sources=sources, staging=staging, intermediate=intermediate, marts=marts)


def collect_loss_summary(
    *,
    root: Path | str = Path.cwd(),
    schema: Pipeline2Schema | None = None,
) -> pl.DataFrame:
    """Run the initial pipeline2 flow and collect the loss summary mart."""

    return build_pipeline2(root=root, schema=schema).marts.loss_summary.collect()


def _scan_path(path: Path, spec: DatasetSpec) -> pl.LazyFrame:
    if not path.exists():
        raise Pipeline2SchemaError(f"{spec.name}: path does not exist: {path}")
    return _scan_file(path, spec)


def _scan_glob(root: Path, spec: DatasetSpec) -> pl.LazyFrame:
    if spec.glob is None:
        raise Pipeline2SchemaError(f"{spec.name}: glob is required")
    paths = sorted(glob(str(root / spec.glob)))
    if not paths:
        raise Pipeline2SchemaError(f"{spec.name}: glob matched no files: {spec.glob}")
    if spec.format == "parquet":
        return pl.scan_parquet(paths)
    if spec.format == "csv":
        return pl.scan_csv(paths, schema_overrides=spec.pl_schema, try_parse_dates=True)
    raise Pipeline2SchemaError(f"{spec.name}: unsupported scan format: {spec.format}")


def _scan_file(path: Path, spec: DatasetSpec) -> pl.LazyFrame:
    if spec.format == "parquet":
        return pl.scan_parquet(path)
    if spec.format == "csv":
        return pl.scan_csv(path, schema_overrides=spec.pl_schema, try_parse_dates=True)
    raise Pipeline2SchemaError(f"{spec.name}: unsupported scan format: {spec.format}")
