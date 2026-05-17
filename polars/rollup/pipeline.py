"""Orchestration for the isolated, schema-driven pipeline Polars flow."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from rollup.intermediate.pipeline import build_selected_losses
from rollup.marts.pipeline import build_loss_summary
from rollup.pipeline_schema import (
    PipelineSchema,
    load_pipeline_schema,
    validate_input_contracts,
    validate_columns,
)
from rollup.reports.pipeline import prepare_summary_ep_stats
from rollup.staging.pipeline import (
    build_normalized_ylt,
    load_configured_sources,
    load_dataset,
    selected_analyses_spec,
    stage_selected_analyses,
)


@dataclass(frozen=True)
class PipelineSources:
    """Source LazyFrames after YAML-backed column validation."""

    selected_analyses: pl.LazyFrame
    raw_ylt: tuple[pl.LazyFrame, ...]
    datasets: dict[str, pl.LazyFrame] = field(default_factory=dict)


@dataclass(frozen=True)
class PipelineStaging:
    """Staging models for the initial pipeline flow."""

    selected_analyses: pl.LazyFrame
    normalized_ylt: pl.LazyFrame


@dataclass(frozen=True)
class PipelineIntermediate:
    """Intermediate models for the initial pipeline flow."""

    selected_losses: pl.LazyFrame


@dataclass(frozen=True)
class PipelineMarts:
    """Mart models for the initial pipeline flow."""

    loss_summary: pl.LazyFrame


@dataclass(frozen=True)
class PipelineReports:
    """Report hooks and summary frames for the pipeline flow."""

    summary_ep_stats: pl.LazyFrame


@dataclass(frozen=True)
class PipelineModels:
    """All models produced by the small linear pipeline DAG."""

    sources: PipelineSources
    staging: PipelineStaging
    intermediate: PipelineIntermediate
    marts: PipelineMarts
    reports: PipelineReports


def build_sources(
    *,
    root: Path | str = Path.cwd(),
    schema: PipelineSchema | None = None,
) -> PipelineSources:
    """Load and validate the source frames needed by the initial pipeline flow."""

    loaded_schema = schema or load_pipeline_schema()
    staged_sources = load_configured_sources(root=root, schema=loaded_schema)
    return PipelineSources(
        selected_analyses=staged_sources.selected_analyses,
        raw_ylt=staged_sources.raw_ylt,
        datasets=staged_sources.datasets,
    )


def preflight_pipeline_boundary(sources: PipelineSources, schema: PipelineSchema) -> None:
    """Validate source contracts before any pipeline transformations run."""

    source_map = sources.datasets or {
        "selected_analyses": sources.selected_analyses,
        "raw_risklink_ylt": sources.raw_ylt[0],
        "raw_verisk_ylt": sources.raw_ylt[1],
    }
    validate_input_contracts(source_map, schema, strict=True)


def build_staging(sources: PipelineSources, schema: PipelineSchema) -> PipelineStaging:
    """Build staging models from already-validated source frames."""

    risklink, verisk = sources.raw_ylt
    selected_analyses = stage_selected_analyses(sources.selected_analyses)
    normalized_ylt = build_normalized_ylt(risklink, verisk)
    validate_columns(selected_analyses, schema.dataset("stg_selected_analyses"), strict=True)
    validate_columns(normalized_ylt, schema.dataset("stg_normalized_ylt"), strict=True)
    return PipelineStaging(selected_analyses=selected_analyses, normalized_ylt=normalized_ylt)


def build_intermediate(
    sources: PipelineSources,
    staging: PipelineStaging,
    schema: PipelineSchema,
) -> PipelineIntermediate:
    """Filter staged losses to the selected analysis allow-list."""

    selected_losses = build_selected_losses(staging.normalized_ylt, staging.selected_analyses)
    validate_columns(selected_losses, schema.dataset("int_selected_losses"), strict=True)
    return PipelineIntermediate(selected_losses=selected_losses)


def build_marts(intermediate: PipelineIntermediate, schema: PipelineSchema) -> PipelineMarts:
    """Build the initial pipeline mart output."""

    loss_summary = build_loss_summary(intermediate.selected_losses)
    validate_columns(loss_summary, schema.dataset("mart_loss_summary"), strict=True)
    return PipelineMarts(loss_summary=loss_summary)


def build_reports(marts: PipelineMarts) -> PipelineReports:
    """Build report helper frames from mart outputs."""

    return PipelineReports(summary_ep_stats=prepare_summary_ep_stats(marts.loss_summary))


def build_pipeline(
    *,
    root: Path | str = Path.cwd(),
    schema: PipelineSchema | None = None,
) -> PipelineModels:
    """Build the full initial pipeline DAG without collecting it."""

    loaded_schema = schema or load_pipeline_schema()
    sources = build_sources(root=root, schema=loaded_schema)
    preflight_pipeline_boundary(sources, loaded_schema)
    staging = build_staging(sources, loaded_schema)
    intermediate = build_intermediate(sources, staging, loaded_schema)
    marts = build_marts(intermediate, loaded_schema)
    reports = build_reports(marts)
    return PipelineModels(sources=sources, staging=staging, intermediate=intermediate, marts=marts, reports=reports)


def collect_loss_summary(
    *,
    root: Path | str = Path.cwd(),
    schema: PipelineSchema | None = None,
) -> pl.DataFrame:
    """Run the initial pipeline flow and collect the loss summary mart."""

    return build_pipeline(root=root, schema=schema).marts.loss_summary.collect()
