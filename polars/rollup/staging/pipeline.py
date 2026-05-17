"""Pipeline staging source loading and light projection query functions."""

from __future__ import annotations

from dataclasses import dataclass
from glob import glob
from pathlib import Path

import polars as pl

from rollup.pipeline_schema import DatasetSpec, PipelineSchema, PipelineSchemaError, validate_columns


@dataclass(frozen=True)
class StagedSources:
    """Raw source LazyFrames loaded for validation and staged layer entry."""

    datasets: dict[str, pl.LazyFrame]

    @property
    def selected_analyses(self) -> pl.LazyFrame:
        return self.datasets["selected_analyses"]

    @property
    def raw_ylt(self) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        return self.datasets["raw_risklink_ylt"], self.datasets["raw_verisk_ylt"]


def selected_analyses_spec(schema: PipelineSchema, *, root: Path | str = Path.cwd()) -> DatasetSpec:
    """Return the required selected analysis allow-list spec."""

    root_path = Path(root)
    selected = schema.dataset("selected_analyses")
    if selected.path is not None and (root_path / selected.path).exists():
        return selected

    raise PipelineSchemaError("selected_analyses is required")


def load_dataset(spec: DatasetSpec, *, root: Path | str = Path.cwd()) -> pl.LazyFrame:
    """Scan a YAML-declared dataset lazily without applying transformations."""

    root_path = Path(root)
    if spec.path is not None:
        frame = _scan_path(root_path / spec.path, spec)
    elif spec.glob is not None:
        frame = _scan_glob(root_path, spec)
    else:
        raise PipelineSchemaError(f"{spec.name}: expected path or glob")

    validate_columns(frame, spec, strict=True)
    return frame


def load_configured_sources(
    *,
    root: Path | str = Path.cwd(),
    schema: PipelineSchema,
) -> StagedSources:
    """Load every configured source input as lazy frames for preflight validation."""

    datasets: dict[str, pl.LazyFrame] = {}
    for spec in schema.datasets.values():
        if spec.role != "source":
            continue
        if not _source_exists(spec, root=Path(root)):
            if spec.required:
                raise PipelineSchemaError(f"{spec.name}: required source is missing")
            continue
        datasets[spec.name] = load_dataset(spec, root=root)
    return StagedSources(datasets=datasets)


def stage_selected_analyses(selected_analyses: pl.LazyFrame) -> pl.LazyFrame:
    """Project the selected analysis allow-list into its staging shape."""

    return selected_analyses.select(
        pl.col("vendor").cast(pl.String),
        pl.col("analysis_id").cast(pl.String),
    )


def stage_risklink_ylt(raw_risklink_ylt: pl.LazyFrame) -> pl.LazyFrame:
    """Project raw RiskLink YLT rows into the canonical pipeline YLT shape."""

    return raw_risklink_ylt.select(
        pl.lit("risklink").alias("vendor"),
        pl.col("anlsid").cast(pl.String).alias("analysis_id"),
        pl.col("yearid").alias("year_id"),
        pl.col("eventid").alias("event_id"),
        pl.col("loss").cast(pl.Float64).alias("loss"),
    )


def stage_verisk_ylt(raw_verisk_ylt: pl.LazyFrame) -> pl.LazyFrame:
    """Project raw Verisk YLT rows into the canonical pipeline YLT shape."""

    return raw_verisk_ylt.select(
        pl.lit("verisk").alias("vendor"),
        pl.col("Analysis").cast(pl.String).alias("analysis_id"),
        pl.col("YearID").alias("year_id"),
        pl.col("EventID").alias("event_id"),
        pl.col("GroundUpLoss").cast(pl.Float64).alias("loss"),
    )


def build_normalized_ylt(
    raw_risklink_ylt: pl.LazyFrame,
    raw_verisk_ylt: pl.LazyFrame,
) -> pl.LazyFrame:
    """Union staged vendor YLT models into one normalized YLT model."""

    return pl.concat(
        [stage_risklink_ylt(raw_risklink_ylt), stage_verisk_ylt(raw_verisk_ylt)],
        how="vertical",
    )


def _source_exists(spec: DatasetSpec, *, root: Path) -> bool:
    if spec.path is not None:
        return (root / spec.path).exists()
    if spec.glob is not None:
        return bool(glob(str(root / spec.glob)))
    return False


def _scan_path(path: Path, spec: DatasetSpec) -> pl.LazyFrame:
    if not path.exists():
        raise PipelineSchemaError(f"{spec.name}: path does not exist: {path}")
    return _scan_file(path, spec)


def _scan_glob(root: Path, spec: DatasetSpec) -> pl.LazyFrame:
    if spec.glob is None:
        raise PipelineSchemaError(f"{spec.name}: glob is required")
    paths = sorted(glob(str(root / spec.glob)))
    if not paths:
        raise PipelineSchemaError(f"{spec.name}: glob matched no files: {spec.glob}")
    if spec.format == "parquet":
        return pl.scan_parquet(paths)
    if spec.format == "csv":
        return pl.scan_csv(paths, schema_overrides=spec.pl_schema, try_parse_dates=True)
    raise PipelineSchemaError(f"{spec.name}: unsupported scan format: {spec.format}")


def _scan_file(path: Path, spec: DatasetSpec) -> pl.LazyFrame:
    if spec.format == "parquet":
        return pl.scan_parquet(path)
    if spec.format == "csv":
        return pl.scan_csv(path, schema_overrides=spec.pl_schema, try_parse_dates=True)
    raise PipelineSchemaError(f"{spec.name}: unsupported scan format: {spec.format}")
