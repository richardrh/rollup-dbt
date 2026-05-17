"""YAML-backed Polars pipeline2 flow.

The application boundary preflights source file schemas from the data-side YAML
manifests before the small linear DAG runs: source loading -> staging ->
intermediate -> mart.
"""

from __future__ import annotations

from dataclasses import dataclass
from glob import glob
from pathlib import Path

import polars as pl

from rollup.pipeline2_schema import (
    DatasetSpec,
    Pipeline2Schema,
    Pipeline2SchemaError,
    load_pipeline2_schema,
    validate_columns,
)


@dataclass(frozen=True)
class Pipeline2Sources:
    """Source LazyFrames after YAML-backed column validation."""

    selected_analyses: pl.LazyFrame
    raw_ylt: tuple[pl.LazyFrame, ...]


@dataclass(frozen=True)
class Pipeline2Staging:
    """Staging models for the initial pipeline2 flow."""

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
    """Scan a YAML-declared dataset with defensive column validation."""

    root_path = Path(root)
    if spec.path is not None:
        frame = _scan_path(root_path / spec.path, spec)
    elif spec.glob is not None:
        frame = _scan_glob(root_path, spec)
    else:
        raise Pipeline2SchemaError(f"{spec.name}: expected path or glob")

    validate_columns(frame, spec, strict=True)
    return frame


def preflight_pipeline2_inputs(
    *,
    root: Path | str = Path.cwd(),
    schema: Pipeline2Schema | None = None,
) -> Pipeline2Schema:
    """Validate source file schemas at the application boundary before the DAG.

    Parquet files expose physical schema metadata, so dtype mismatches are caught
    without collecting data. CSV files have no physical dtype metadata; preflight
    validates headers strictly and applies the YAML-declared Polars dtypes as the
    planned read schema before any pipeline transformation is built.
    """

    loaded_schema = schema or load_pipeline2_schema()
    root_path = Path(root)
    selected_spec = selected_analyses_spec(loaded_schema, root=root_path)
    for spec in _source_specs_for_preflight(loaded_schema, selected_spec, root=root_path):
        _preflight_dataset_files(root_path, spec)
    return loaded_schema


def selected_analyses_spec(schema: Pipeline2Schema, *, root: Path | str = Path.cwd()) -> DatasetSpec:
    """Return the required first-class selected_analyses source spec."""

    root_path = Path(root)
    selected = schema.dataset("selected_analyses")
    if selected.path is not None and (root_path / selected.path).exists():
        return selected

    raise Pipeline2SchemaError(f"selected_analyses is required: {root_path / selected.path}")


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


def build_staging(sources: Pipeline2Sources, schema: Pipeline2Schema) -> Pipeline2Staging:
    """Build staging models with source projections inline and visible."""

    risklink, verisk = sources.raw_ylt
    normalized_ylt = pl.concat(
        [
            risklink.select(
                pl.lit("risklink").alias("vendor"),
                pl.col("anlsid").cast(pl.String).alias("analysis_id"),
                pl.col("yearid").alias("year_id"),
                pl.col("eventid").alias("event_id"),
                pl.col("loss").cast(pl.Float64).alias("loss"),
            ),
            verisk.select(
                pl.lit("verisk").alias("vendor"),
                pl.col("Analysis").cast(pl.String).alias("analysis_id"),
                pl.col("YearID").alias("year_id"),
                pl.col("EventID").alias("event_id"),
                pl.col("GroundUpLoss").cast(pl.Float64).alias("loss"),
            ),
        ],
        how="vertical",
    )
    validate_columns(normalized_ylt, schema.dataset("stg_normalized_ylt"), strict=True)
    return Pipeline2Staging(normalized_ylt=normalized_ylt)


def build_intermediate(
    sources: Pipeline2Sources,
    staging: Pipeline2Staging,
    schema: Pipeline2Schema,
) -> Pipeline2Intermediate:
    """Filter staged losses to the selected analysis allow-list."""

    selected_losses = staging.normalized_ylt.join(
        sources.selected_analyses,
        on=["vendor", "analysis_id"],
        how="inner",
    )
    validate_columns(selected_losses, schema.dataset("int_selected_losses"), strict=True)
    return Pipeline2Intermediate(selected_losses=selected_losses)


def build_marts(intermediate: Pipeline2Intermediate, schema: Pipeline2Schema) -> Pipeline2Marts:
    """Build the initial pipeline2 mart output."""

    loss_summary = (
        intermediate.selected_losses.group_by("vendor", "analysis_id")
        .agg(
            pl.col("loss").sum().alias("total_loss"),
            pl.len().cast(pl.UInt32).alias("event_count"),
        )
        .sort("vendor", "analysis_id")
    )
    validate_columns(loss_summary, schema.dataset("mart_loss_summary"), strict=True)
    return Pipeline2Marts(loss_summary=loss_summary)


def build_pipeline2(
    *,
    root: Path | str = Path.cwd(),
    schema: Pipeline2Schema | None = None,
) -> Pipeline2Models:
    """Build the full initial pipeline2 DAG without collecting it."""

    loaded_schema = preflight_pipeline2_inputs(root=root, schema=schema)
    sources = build_sources(root=root, schema=loaded_schema)
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


def _source_specs_for_preflight(
    schema: Pipeline2Schema,
    selected_spec: DatasetSpec,
    *,
    root: Path,
) -> tuple[DatasetSpec, ...]:
    specs = [selected_spec]
    for spec in schema.datasets.values():
        if spec.role != "source" or spec.name == "selected_analyses":
            continue
        if spec.required or _dataset_has_files(root, spec):
            specs.append(spec)
    return tuple(specs)


def _preflight_dataset_files(root: Path, spec: DatasetSpec) -> None:
    for path in _dataset_paths(root, spec, required=spec.required):
        frame = _scan_file(path, spec)
        try:
            validate_columns(frame, spec, strict=True)
        except Pipeline2SchemaError as exc:
            raise Pipeline2SchemaError(f"{spec.name}: {path}: {exc}") from exc


def _dataset_has_files(root: Path, spec: DatasetSpec) -> bool:
    return bool(_dataset_paths(root, spec, required=False))


def _dataset_paths(root: Path, spec: DatasetSpec, *, required: bool) -> tuple[Path, ...]:
    if spec.path is not None:
        path = root / spec.path
        if path.exists():
            return (path,)
        if required:
            raise Pipeline2SchemaError(f"{spec.name}: path does not exist: {path}")
        return ()

    if spec.glob is not None:
        paths = tuple(Path(path) for path in sorted(glob(str(root / spec.glob))))
        if paths or not required:
            return paths
        raise Pipeline2SchemaError(f"{spec.name}: glob matched no files: {spec.glob}")

    raise Pipeline2SchemaError(f"{spec.name}: expected path or glob")
