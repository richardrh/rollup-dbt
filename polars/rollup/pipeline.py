"""Orchestration for the isolated, schema-driven pipeline Polars flow."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from rollup.intermediate.pipeline import build_intermediate_losses
from rollup.marts.pipeline import build_analysis_loss_summary, build_blended_loss_summary, build_event_loss_fanout
from rollup.pipeline_schema import (
    PipelineSchema,
    load_pipeline_schema,
    validate_input_contracts,
    validate_columns,
)
from rollup.reports.pipeline import collect_report_artifact, prepare_summary_ep_stats, write_xlsx_report
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
    """Intermediate models for business joins and VOR adjustments."""

    selected_losses: pl.LazyFrame
    enriched_losses: pl.LazyFrame
    adjusted_losses: pl.LazyFrame
    blended_losses: pl.LazyFrame


@dataclass(frozen=True)
class PipelineMarts:
    """Fanout mart models for output tables."""

    loss_summary: pl.LazyFrame
    analysis_loss_summary: pl.LazyFrame
    event_loss_fanout: pl.LazyFrame
    blended_loss_summary: pl.LazyFrame


@dataclass(frozen=True)
class PipelineReports:
    """Report hooks and summary frames for the pipeline flow."""

    summary_ep_stats: pl.LazyFrame
    artifact: dict[str, pl.DataFrame] | None = None
    xlsx_path: Path | None = None


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
    """Build business joins, forecast factors, FX, EUWS, and blended losses."""

    selected_losses, enriched_losses, adjusted_losses, blended_losses = build_intermediate_losses(
        staging.normalized_ylt,
        staging.selected_analyses,
        sources.datasets["analyses"],
        sources.datasets["perils"],
        sources.datasets["lobs"],
        sources.datasets["forecast_factors"],
        sources.datasets["fx_rates"],
        sources.datasets["euws_rate_factors"],
        sources.datasets["blending_weights"],
    )
    validate_columns(selected_losses, schema.dataset("int_selected_losses"), strict=True)
    validate_columns(enriched_losses, schema.dataset("int_enriched_losses"), strict=True)
    validate_columns(adjusted_losses, schema.dataset("int_adjusted_losses"), strict=True)
    validate_columns(blended_losses, schema.dataset("int_blended_losses"), strict=True)
    return PipelineIntermediate(
        selected_losses=selected_losses,
        enriched_losses=enriched_losses,
        adjusted_losses=adjusted_losses,
        blended_losses=blended_losses,
    )


def build_marts(intermediate: PipelineIntermediate, schema: PipelineSchema) -> PipelineMarts:
    """Build fanout mart outputs."""

    analysis_loss_summary = build_analysis_loss_summary(intermediate.adjusted_losses)
    event_loss_fanout = build_event_loss_fanout(intermediate.adjusted_losses)
    blended_loss_summary = build_blended_loss_summary(intermediate.blended_losses)
    loss_summary = analysis_loss_summary.select("vendor", "analysis_id", "total_loss", "event_count")
    validate_columns(loss_summary, schema.dataset("mart_loss_summary"), strict=True)
    validate_columns(analysis_loss_summary, schema.dataset("mart_analysis_loss_summary"), strict=True)
    validate_columns(event_loss_fanout, schema.dataset("mart_event_loss_fanout"), strict=True)
    validate_columns(blended_loss_summary, schema.dataset("mart_blended_loss_summary"), strict=True)
    return PipelineMarts(
        loss_summary=loss_summary,
        analysis_loss_summary=analysis_loss_summary,
        event_loss_fanout=event_loss_fanout,
        blended_loss_summary=blended_loss_summary,
    )


def build_reports(marts: PipelineMarts, *, report_path: Path | str | None = None) -> PipelineReports:
    """Build report helper frames from mart outputs."""

    summary_ep_stats = prepare_summary_ep_stats(marts.loss_summary)
    artifact = collect_report_artifact(loss_summary=marts.loss_summary, summary_ep_stats=summary_ep_stats)
    xlsx_path = Path(report_path) if report_path is not None else None
    if xlsx_path is not None:
        write_xlsx_report(
            path=xlsx_path,
            loss_summary=artifact["loss_summary"],
            summary_ep_stats=artifact["summary_ep_stats"],
        )
    return PipelineReports(summary_ep_stats=summary_ep_stats, artifact=artifact, xlsx_path=xlsx_path)


def build_pipeline(
    *,
    root: Path | str = Path.cwd(),
    schema: PipelineSchema | None = None,
    report_path: Path | str | None = None,
) -> PipelineModels:
    """Build the full initial pipeline DAG without collecting it."""

    loaded_schema = schema or load_pipeline_schema()
    sources = build_sources(root=root, schema=loaded_schema)
    preflight_pipeline_boundary(sources, loaded_schema)
    staging = build_staging(sources, loaded_schema)
    intermediate = build_intermediate(sources, staging, loaded_schema)
    marts = build_marts(intermediate, loaded_schema)
    reports = build_reports(marts, report_path=report_path)
    return PipelineModels(sources=sources, staging=staging, intermediate=intermediate, marts=marts, reports=reports)


def collect_loss_summary(
    *,
    root: Path | str = Path.cwd(),
    schema: PipelineSchema | None = None,
) -> pl.DataFrame:
    """Run the initial pipeline flow and collect the loss summary mart."""

    return build_pipeline(root=root, schema=schema).marts.loss_summary.collect()
