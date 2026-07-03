from __future__ import annotations
# mypy: ignore-errors

import logging
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from rollup.columns import Col, FanoutCol, RawCol
from rollup.config import RollupConfig


logger = logging.getLogger(__name__)


@contextmanager
def logged_phase(phase: str) -> Iterator[None]:
    started = time.perf_counter()
    logger.info("start phase=%s", phase, extra={"event": "phase_start", "phase": phase})
    try:
        yield
    except Exception:
        elapsed_seconds = time.perf_counter() - started
        logger.exception(
            "failed phase=%s elapsed=%.2fs",
            phase,
            elapsed_seconds,
            extra={"event": "phase_failed", "phase": phase, "elapsed_seconds": elapsed_seconds},
        )
        raise
    elapsed_seconds = time.perf_counter() - started
    logger.info(
        "done phase=%s elapsed=%.2fs",
        phase,
        elapsed_seconds,
        extra={"event": "phase_done", "phase": phase, "elapsed_seconds": elapsed_seconds},
    )


@dataclass(frozen=True)
class YltFrames:
    verisk: pl.LazyFrame
    risklink: pl.LazyFrame


@dataclass(frozen=True)
class YltValidationResult:
    frames: YltFrames
    report: pl.DataFrame


@dataclass(frozen=True)
class EpSummaryValidationResult:
    frame: pl.LazyFrame
    report: pl.DataFrame


@dataclass(frozen=True)
class SeedValidationResult:
    frames: dict[str, pl.DataFrame]
    report: pl.DataFrame

def load_verisk_events(
    data_root: Path | str = "data",
    config: RollupConfig | None = None,
) -> pl.LazyFrame:
    config = config or RollupConfig()
    return pl.scan_parquet(config.inputs.verisk_events_path(data_root)).select(
        pl.col(RawCol.EventID).alias(Col.model_event_id),
        pl.col(RawCol.ModelID).alias(Col.model_code),
        pl.col(RawCol.Event).alias(Col.event_id),
        pl.col(RawCol.Year).alias(Col.year_id),
        pl.col(RawCol.Day).alias(Col.event_day),
    )


def load_risklink_flood_events(
    data_root: Path | str = "data",
    config: RollupConfig | None = None,
) -> pl.LazyFrame:
    config = config or RollupConfig()
    return (
        pl.scan_parquet(config.inputs.risklink_events_path(data_root))
        .group_by(FanoutCol.ModelEventID, RawCol.ModelOccurrenceYear, RawCol.RegionPerilID)
        .agg(pl.col(RawCol.ModelOccurrenceDate).min().alias(Col.model_occurrence_date))
        .select(
            pl.col(FanoutCol.ModelEventID).cast(pl.Int64).alias(Col.event_id),
            pl.col(RawCol.ModelOccurrenceYear).cast(pl.Int64).alias(Col.model_occurrence_year),
            pl.col(RawCol.RegionPerilID).cast(pl.Int64).alias(Col.region_peril_id),
            pl.col(Col.model_occurrence_date)
            .dt.ordinal_day()
            .cast(pl.Int64)
            .alias(Col.risklink_event_day),
        )
    )


def load_validated_ep_summary_frames(
    data_root: Path | str = "data",
) -> EpSummaryValidationResult:
    data_root = Path(data_root)
    folder = data_root / "ep_summaries"
    paths = sorted(folder.rglob("*.long.csv"))

    if not paths:
        return EpSummaryValidationResult(
            frame=pl.LazyFrame(),
            report=pl.DataFrame(
                [
                    {
                        "filename": None,
                        "path": str(folder),
                        "valid": False,
                        "row_count": None,
                        "error": "no EP summary .long.csv files found",
                    }
                ]
            ),
        )

    frame = pl.scan_csv(str(folder / "**" / "*.long.csv"))
    rows: list[dict[str, object]] = []

    for file_path in paths:
        file_frame = pl.scan_csv(file_path)
        errors: list[str] = []
        row_count = None

        if not errors:
            row_count = file_frame.select(pl.len().alias(Col.row_count)).collect().item()

        rows.append(
            {
                "filename": file_path.name,
                "path": str(file_path),
                "valid": not errors,
                "row_count": row_count,
                "error": None,
            }
        )

    return EpSummaryValidationResult(frame=frame, report=pl.DataFrame(rows))


def load_validated_seed_frames(data_root: Path | str = "data") -> SeedValidationResult:
    data_root = Path(data_root)

    frames: dict[str, pl.DataFrame] = {}
    rows: list[dict[str, object]] = []

    for file_path in sorted((data_root / "seeds").rglob("*.csv")):
        filename = file_path.name

        try:
            validated = pl.read_csv(file_path)
        except Exception as exc:
            rows.append(
                {
                    "filename": filename,
                    "path": str(file_path),
                    "valid": False,
                    "row_count": None,
                    "error": str(exc),
                }
            )
            continue

        frames[filename] = validated
        rows.append(
            {
                "filename": filename,
                "path": str(file_path),
                "valid": True,
                "row_count": validated.height,
                "error": None,
            }
        )

    return SeedValidationResult(frames=frames, report=pl.DataFrame(rows))


@dataclass(frozen=True)
class NormalizedYltFrames:
    verisk: pl.LazyFrame
    risklink: pl.LazyFrame


@dataclass(frozen=True)
class EnrichedYltFrames:
    verisk: pl.LazyFrame
    risklink: pl.LazyFrame
    combined: pl.LazyFrame


@dataclass(frozen=True)
class StagedEpSummaries:
    enriched: pl.LazyFrame
    selected: pl.LazyFrame
    selected_dialsup: pl.LazyFrame


@dataclass(frozen=True)
class JoinedEpSummaries:
    enriched: pl.LazyFrame
    verisk: pl.LazyFrame
    risklink: pl.LazyFrame
    joined: pl.LazyFrame


@dataclass(frozen=True)
class EpBlendingTargets:
    target_points: pl.LazyFrame
    weights: pl.LazyFrame
    blended: pl.LazyFrame


@dataclass(frozen=True)
class PipelineStage:
    frames: dict[str, pl.DataFrame | pl.LazyFrame]


@dataclass(frozen=True)
class PipelineRunResult:
    seeds: PipelineStage
    staging: PipelineStage
    intermediate: PipelineStage
    marts: PipelineStage


@dataclass(frozen=True)
class PipelineValidationInputs:
    seeds: SeedValidationResult
    ylts: YltValidationResult
    ep_summaries: EpSummaryValidationResult
    coverage_report: pl.DataFrame


_INPUT_YLT_AAL_SUMMARY_SCHEMA = [
    Col.vendor,
    Col.rollup_lob,
    Col.rollup_peril,
    Col.modelled_lob,
    Col.modelled_peril,
    Col.row_count,
    Col.loss_sum,
    "simulation_count",
    "raw_aal",
]


def load_validated_ylt_frames(data_root: Path | str = "data") -> YltValidationResult:
    data_root = Path(data_root)
    rows: list[dict[str, object]] = []

    def scan_vendor(vendor: str) -> pl.LazyFrame:
        folder = data_root / "ylt" / vendor
        paths = sorted(folder.glob("*.parquet"))
        if not paths:
            rows.append({"filename": None, "path": str(folder), "valid": False, "row_count": None, "error": f"no {vendor} parquet files found"})
            return pl.LazyFrame()
        for path in paths:
            try:
                row_count = pl.scan_parquet(path).select(pl.len()).collect().item()
                rows.append({"filename": path.name, "path": str(path), "valid": True, "row_count": row_count, "error": None})
            except Exception as exc:
                rows.append({"filename": path.name, "path": str(path), "valid": False, "row_count": None, "error": str(exc)})
        return pl.scan_parquet(str(folder / "*.parquet"))

    return YltValidationResult(
        frames=YltFrames(verisk=scan_vendor("verisk"), risklink=scan_vendor("risklink")),
        report=pl.DataFrame(rows),
    )


def empty_modelled_dimension_coverage_report() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "severity": pl.String,
            "direction": pl.String,
            "source_group": pl.String,
            "dimension": pl.String,
            "field": pl.String,
            "path": pl.String,
            "value": pl.String,
            "count": pl.Int64,
            "message": pl.String,
            "error": pl.String,
        }
    )


def _coverage_rows(groups: pl.LazyFrame, **values: object) -> pl.LazyFrame:
    if "message" in values and "error" not in values:
        values["error"] = values["message"]
    values = {key: str(value) if isinstance(value, Path) else value for key, value in values.items()}
    return groups.with_columns(
        *(pl.lit(value).alias(key) for key, value in values.items())
    )


def _verisk_string(column: RawCol) -> pl.Expr:
    return pl.col(column).cast(pl.String).str.strip_chars()


def modelled_dimension_coverage_report(
    seeds: SeedValidationResult,
    ylt: YltValidationResult,
    ep_summaries: EpSummaryValidationResult,
    data_root: Path | str = "data",
) -> pl.DataFrame:
    if "lobs.csv" not in seeds.frames or "perils.csv" not in seeds.frames:
        return empty_modelled_dimension_coverage_report()
    data_root = Path(data_root)
    seed_lobs = seeds.frames["lobs.csv"].lazy().select(Col.modelled_lob).unique()
    seed_perils = seeds.frames["perils.csv"].lazy().select(Col.modelled_peril).unique()
    reports: list[pl.LazyFrame] = []

    sources = [
        (
            "verisk_ylt",
            ylt.frames.verisk.filter(_verisk_string(RawCol.CatalogTypeCode) == "STC").select(
                _verisk_string(RawCol.ExposureAttribute).alias(Col.modelled_lob),
                _verisk_string(RawCol.Analysis).alias(Col.modelled_peril),
            ),
        ),
        (
            "ep_summaries",
            ep_summaries.frame.select(Col.modelled_lob, Col.modelled_peril),
        ),
    ]
    for source_group, source in sources:
        for dimension, seed_values in (
            (Col.modelled_lob, seed_lobs),
            (Col.modelled_peril, seed_perils),
        ):
            missing = (
                source.select(pl.col(dimension).cast(pl.String).alias("value"))
                .drop_nulls()
                .unique()
                .join(seed_values.select(pl.col(dimension).cast(pl.String).alias("value")), on="value", how="anti")
                .with_columns(pl.len().over("value").alias("count"))
            )
            reports.append(
                _coverage_rows(
                    missing,
                    severity="error",
                    direction="input_missing_from_seed",
                    source_group=source_group,
                    dimension=dimension,
                    field=dimension,
                    path=data_root,
                    message=f"{dimension} value is missing from seed file",
                )
            )
    if not reports:
        return empty_modelled_dimension_coverage_report()
    report = pl.concat(reports, how="diagonal_relaxed").collect()
    if report.is_empty():
        return empty_modelled_dimension_coverage_report()
    return report.sort(["severity", "direction", "source_group", "dimension", "value"])


def ensure_modelled_dimension_coverage(report: pl.DataFrame) -> None:
    if "severity" in report.columns and report.filter(pl.col("severity") == "error").height:
        raise ValueError("rollup validate failed: modelled LOB/peril coverage validation failed")


def load_pipeline_validation_inputs(data_root: Path | str = "data") -> PipelineValidationInputs:
    seeds = load_validated_seed_frames(data_root)
    ylts = load_validated_ylt_frames(data_root)
    ep_summaries = load_validated_ep_summary_frames(data_root)
    coverage_report = modelled_dimension_coverage_report(seeds, ylts, ep_summaries, data_root)
    return PipelineValidationInputs(
        seeds=seeds,
        ylts=ylts,
        ep_summaries=ep_summaries,
        coverage_report=coverage_report,
    )


def ensure_pipeline_validation_inputs(inputs: PipelineValidationInputs) -> None:
    if _validation_has_blocking_errors(inputs):
        raise ValueError("pipeline input validation failed")


def ylt_loss_validation_summary(data_root: Path | str = "data") -> pl.DataFrame:
    return load_validated_ylt_frames(data_root).report


def normalize_ylt(ylt: YltValidationResult) -> NormalizedYltFrames:
    verisk = ylt.frames.verisk.filter(_verisk_string(RawCol.CatalogTypeCode) == "STC").select(
        pl.lit("verisk").alias(Col.vendor),
        _verisk_string(RawCol.Analysis).alias(Col.analysis_id),
        _verisk_string(RawCol.Analysis).alias(Col.modelled_peril),
        _verisk_string(RawCol.ExposureAttribute).alias(Col.modelled_lob),
        pl.col(RawCol.ModelCode).cast(pl.Int64).alias(Col.model_code),
        pl.col(RawCol.YearID).cast(pl.Int64).alias(Col.year_id),
        pl.col(RawCol.EventID).cast(pl.Int64).alias(Col.event_id),
        pl.col(RawCol.GroundUpLoss).cast(pl.Float64).alias(Col.loss),
    )

    risklink = ylt.frames.risklink.select(
        pl.lit("risklink").alias(Col.vendor),
        pl.col(RawCol.anlsid).cast(pl.String).alias(Col.analysis_id),
        pl.lit(None).cast(pl.String).alias(Col.modelled_peril),
        pl.lit(None).cast(pl.String).alias(Col.modelled_lob),
        pl.lit(None).cast(pl.Int64).alias(Col.model_code),
        pl.col(RawCol.yearid).cast(pl.Int64).alias(Col.year_id),
        pl.col(RawCol.eventid).cast(pl.Int64).alias(Col.event_id),
        pl.col(RawCol.loss).cast(pl.Float64).alias(Col.loss),
    )

    return NormalizedYltFrames(verisk=verisk, risklink=risklink)


def enrich_ylt_with_ep_summaries(
    normalized_ylt: NormalizedYltFrames,
    staged_ep_summaries: StagedEpSummaries,
    *,
    use_dialsup_selection: bool = False,
) -> EnrichedYltFrames:
    ep_summary = (
        staged_ep_summaries.selected_dialsup
        if use_dialsup_selection
        else staged_ep_summaries.selected
    )

    verisk_keys = (
        ep_summary.filter(pl.col(Col.vendor) == "verisk")
        .select(
            Col.vendor,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.rollup_lob,
            Col.rollup_peril,
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.base_model,
            Col.selection_priority,
            Col.is_dialsup,
            Col.is_euws,
            Col.cds_cat_class_name,
            Col.class_,
            Col.office,
            Col.currency,
        )
        .unique()
    )
    verisk = normalized_ylt.verisk.join(
        verisk_keys,
        on=[Col.vendor, Col.modelled_lob, Col.modelled_peril],
        how="inner",
    ).select(
        Col.vendor,
        Col.analysis_id,
        Col.modelled_lob,
        Col.modelled_peril,
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.blend_subregion_peril_id,
        Col.base_model,
        Col.selection_priority,
        Col.is_dialsup,
        Col.is_euws,
        Col.cds_cat_class_name,
        Col.class_,
        Col.office,
        Col.currency,
        Col.model_code,
        Col.year_id,
        Col.event_id,
        Col.loss,
    )

    risklink_lookup = (
        ep_summary.filter(pl.col(Col.vendor) == "risklink")
        .select(
            Col.vendor,
            Col.analysis_id,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.rollup_lob,
            Col.rollup_peril,
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.base_model,
            Col.selection_priority,
            Col.is_dialsup,
            Col.is_euws,
            Col.cds_cat_class_name,
            Col.class_,
            Col.office,
            Col.currency,
        )
        .unique()
    )
    risklink = (
        normalized_ylt.risklink.drop(Col.modelled_lob, Col.modelled_peril)
        .join(risklink_lookup, on=[Col.vendor, Col.analysis_id], how="inner")
        .select(
            Col.vendor,
            Col.analysis_id,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.rollup_lob,
            Col.rollup_peril,
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.base_model,
            Col.selection_priority,
            Col.is_dialsup,
            Col.is_euws,
            Col.cds_cat_class_name,
            Col.class_,
            Col.office,
            Col.currency,
            Col.model_code,
            Col.year_id,
            Col.event_id,
            Col.loss,
        )
    )

    combined = pl.concat([verisk, risklink], how="vertical")

    return EnrichedYltFrames(verisk=verisk, risklink=risklink, combined=combined)


def stage_ep_summaries(
    ep_summaries: EpSummaryValidationResult,
    seeds: SeedValidationResult,
) -> StagedEpSummaries:
    lobs = seeds.frames["lobs.csv"].lazy().select(
        Col.modelled_lob,
        Col.rollup_lob,
        Col.cds_cat_class_name,
        Col.class_,
        Col.office,
        Col.currency,
    )
    perils = seeds.frames["perils.csv"].lazy().select(
        Col.modelled_peril,
        Col.rollup_peril,
        "region",
        "peril",
        Col.region_peril_id,
        Col.blend_subregion_peril_id,
        Col.base_model,
        Col.selection_priority,
        Col.is_dialsup,
        Col.is_euws,
    )

    enriched = (
        ep_summaries.frame.join(lobs, on=Col.modelled_lob, how="left")
        .join(perils, on=Col.modelled_peril, how="left")
        .with_columns(pl.col(Col.selection_priority).fill_null(99))
    )
    selection_keys = [Col.vendor, Col.rollup_lob, Col.rollup_peril]
    selected_modelled_perils = _select_modelled_perils_by_priority(enriched, selection_keys)
    selected_dialsup_modelled_perils = _select_dialsup_modelled_perils(enriched, selection_keys)
    selected = enriched.join(
        selected_modelled_perils,
        on=[*selection_keys, Col.modelled_peril],
        how="inner",
    )
    selected_dialsup = enriched.join(
        selected_dialsup_modelled_perils,
        on=[*selection_keys, Col.modelled_peril],
        how="inner",
    )

    return StagedEpSummaries(enriched=enriched, selected=selected, selected_dialsup=selected_dialsup)


def _select_modelled_perils_by_priority(
    enriched: pl.LazyFrame,
    selection_keys: list[str],
) -> pl.LazyFrame:
    selected_candidates = enriched.select(
        *selection_keys,
        Col.modelled_peril,
        Col.selection_priority,
    ).unique()
    selected_priorities = selected_candidates.group_by(selection_keys).agg(
        pl.col(Col.selection_priority).min()
    )
    return (
        selected_candidates.join(
            selected_priorities,
            on=[*selection_keys, Col.selection_priority],
            how="inner",
        )
        .sort([*selection_keys, Col.selection_priority, Col.modelled_peril])
        .group_by(selection_keys, maintain_order=True)
        .first()
        .select(*selection_keys, Col.modelled_peril)
    )


def _select_dialsup_modelled_perils(
    enriched: pl.LazyFrame,
    selection_keys: list[str],
) -> pl.LazyFrame:
    return (
        enriched.filter(pl.col(Col.is_dialsup) == 1)
        .select(*selection_keys, Col.modelled_peril)
        .unique()
    )


def _validation_has_blocking_errors(inputs: PipelineValidationInputs) -> bool:
    structural_report = pl.concat(
        [inputs.seeds.report, inputs.ylts.report, inputs.ep_summaries.report],
        how="diagonal_relaxed",
    )
    if structural_report.filter(~pl.col("valid")).height:
        return True
    return inputs.coverage_report.filter(pl.col("severity") == "error").height > 0


def _input_ylt_aal_group(frame: pl.LazyFrame, simulation_count: int) -> pl.LazyFrame:
    keys = [
        Col.vendor,
        Col.rollup_lob,
        Col.rollup_peril,
        Col.modelled_lob,
        Col.modelled_peril,
    ]
    return (
        frame.group_by(keys)
        .agg(
            pl.len().cast(pl.Int64).alias(Col.row_count),
            pl.col(Col.loss).sum().cast(pl.Float64).alias(Col.loss_sum),
        )
        .with_columns(
            pl.lit(simulation_count).cast(pl.Int64).alias("simulation_count"),
            (pl.col(Col.loss_sum) / simulation_count).cast(pl.Float64).alias("raw_aal"),
        )
        .select(*_INPUT_YLT_AAL_SUMMARY_SCHEMA)
    )


def empty_input_ylt_aal_by_lob_peril_summary() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            Col.vendor: pl.String,
            Col.rollup_lob: pl.String,
            Col.rollup_peril: pl.String,
            Col.modelled_lob: pl.String,
            Col.modelled_peril: pl.String,
            Col.row_count: pl.Int64,
            Col.loss_sum: pl.Float64,
            "simulation_count": pl.Int64,
            "raw_aal": pl.Float64,
        }
    )


def input_ylt_aal_by_lob_peril_summary(inputs: PipelineValidationInputs) -> pl.DataFrame:
    if _validation_has_blocking_errors(inputs):
        return empty_input_ylt_aal_by_lob_peril_summary()

    required_seeds = {"lobs.csv", "perils.csv"}
    if not required_seeds <= set(inputs.seeds.frames):
        return empty_input_ylt_aal_by_lob_peril_summary()

    lobs = inputs.seeds.frames["lobs.csv"].lazy().select(
        Col.modelled_lob,
        Col.rollup_lob,
    )
    perils = inputs.seeds.frames["perils.csv"].lazy().select(
        Col.modelled_peril,
        Col.rollup_peril,
    )
    verisk = (
        inputs.ylts.frames.verisk.filter(_verisk_string(RawCol.CatalogTypeCode) == "STC")
        .select(
            pl.lit("verisk").alias(Col.vendor),
            _verisk_string(RawCol.ExposureAttribute).alias(Col.modelled_lob),
            _verisk_string(RawCol.Analysis).alias(Col.modelled_peril),
            pl.col(RawCol.GroundUpLoss).cast(pl.Float64).alias(Col.loss),
        )
        .join(lobs, on=Col.modelled_lob, how="inner")
        .join(perils, on=Col.modelled_peril, how="inner")
    )

    staged_ep_summaries = stage_ep_summaries(inputs.ep_summaries, inputs.seeds)
    risklink_lookup = (
        staged_ep_summaries.selected.filter(pl.col(Col.vendor) == "risklink")
        .select(
            Col.vendor,
            Col.analysis_id,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.rollup_lob,
            Col.rollup_peril,
        )
        .unique()
    )
    risklink = (
        inputs.ylts.frames.risklink.select(
            pl.lit("risklink").alias(Col.vendor),
            pl.col(RawCol.anlsid).cast(pl.String).alias(Col.analysis_id),
            pl.col(RawCol.loss).cast(pl.Float64).alias(Col.loss),
        )
        .join(risklink_lookup, on=[Col.vendor, Col.analysis_id], how="inner")
        .select(
            Col.vendor,
            Col.rollup_lob,
            Col.rollup_peril,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.loss,
        )
    )

    return (
        pl.concat(
            [
                _input_ylt_aal_group(verisk, 10_000),
                _input_ylt_aal_group(risklink, 100_000),
            ],
            how="vertical",
        )
        .sort(
            [
                "raw_aal",
                Col.vendor,
                Col.rollup_lob,
                Col.rollup_peril,
                Col.modelled_lob,
                Col.modelled_peril,
            ],
            descending=[True, False, False, False, False, False],
        )
        .collect()
    )


def join_ep_summaries(
    staged_ep_summaries: StagedEpSummaries,
) -> JoinedEpSummaries:
    enriched = staged_ep_summaries.selected

    join_keys = [
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.blend_subregion_peril_id,
        Col.base_model,
        Col.ep_type,
        Col.return_period,
    ]
    verisk = (
        enriched.filter(pl.col(Col.vendor) == "verisk")
        .group_by(join_keys)
        .agg(pl.col(Col.loss).sum().alias(Col.verisk_loss))
    )
    risklink = (
        enriched.filter(pl.col(Col.vendor) == "risklink")
        .group_by(join_keys)
        .agg(pl.col(Col.loss).sum().alias(Col.risklink_loss))
    )
    joined = risklink.join(verisk, on=join_keys, how="full", coalesce=True)

    return JoinedEpSummaries(
        enriched=staged_ep_summaries.enriched,
        verisk=verisk,
        risklink=risklink,
        joined=joined,
    )


def calculate_ep_blending_targets(
    joined_ep_summaries: JoinedEpSummaries,
    seeds: SeedValidationResult,
    config: RollupConfig | None = None,
) -> EpBlendingTargets:
    config = config or RollupConfig()
    target_predicate = pl.lit(False)
    for point in config.blending.target_points:
        target_predicate = target_predicate | (
            (pl.col(Col.ep_type) == point.ep_type)
            & (pl.col(Col.return_period) == point.return_period)
        )
    target_points = joined_ep_summaries.joined.filter(
        target_predicate
    )

    weights = (
        seeds.frames["blending_factors.csv"]
        .lazy()
        .select(
            pl.col(RawCol.RegionPerilID).alias(Col.region_peril_id),
            pl.col(RawCol.SubRegionPerilID).alias(Col.blend_subregion_peril_id),
            pl.col(RawCol.SubRegionPeril).alias(Col.sub_region_peril),
            pl.col(RawCol.AIRBlend).cast(pl.Float64).alias(Col.verisk_weight),
            pl.col(RawCol.RMSBlend).cast(pl.Float64).alias(Col.risklink_weight),
        )
    )

    blended = (
        target_points
        .join(weights, on=[Col.region_peril_id, Col.blend_subregion_peril_id], how="left")
        .with_columns(
            pl.when(pl.col(Col.base_model) == "risklink")
            .then(pl.col(Col.risklink_loss))
            .otherwise(pl.col(Col.verisk_loss))
            .alias(Col.base_model_loss)
        )
        .filter(pl.col(Col.base_model_loss).is_not_null())
        .with_columns(
            (
                pl.col(Col.risklink_loss).is_not_null()
                & pl.col(Col.verisk_loss).is_not_null()
            ).alias("_has_both_vendor_losses")
        )
        .with_columns(
            pl.when(pl.col("_has_both_vendor_losses"))
            .then(pl.col(Col.risklink_loss) * pl.col(Col.risklink_weight))
            .when(pl.col(Col.base_model) == "risklink")
            .then(pl.col(Col.base_model_loss))
            .otherwise(pl.lit(0.0))
            .alias(Col.risklink_blended_contribution),
            pl.when(pl.col("_has_both_vendor_losses"))
            .then(pl.col(Col.verisk_loss) * pl.col(Col.verisk_weight))
            .when(pl.col(Col.base_model) == "verisk")
            .then(pl.col(Col.base_model_loss))
            .otherwise(pl.lit(0.0))
            .alias(Col.verisk_blended_contribution),
        )
        .with_columns(
            pl.when(pl.col("_has_both_vendor_losses"))
            .then(
                pl.col(Col.risklink_blended_contribution)
                + pl.col(Col.verisk_blended_contribution)
            )
            .otherwise(pl.col(Col.base_model_loss))
            .alias(Col.target_loss)
        )
        .with_columns(
            (pl.col(Col.target_loss) / pl.col(Col.base_model_loss)).alias(
                Col.uplift_factor_on_base_model
            )
        )
        .with_columns(
            pl.col(Col.uplift_factor_on_base_model)
            .clip(lower_bound=config.blending.uplift_factor_min, upper_bound=config.blending.uplift_factor_max)
            .alias(Col.uplift_factor_on_base_model)
        )
        .drop("_has_both_vendor_losses")
    )

    return EpBlendingTargets(
        target_points=target_points,
        weights=weights,
        blended=blended,
    )


def _add_rank_columns(ylt: pl.LazyFrame, config: RollupConfig | None = None) -> pl.LazyFrame:
    config = config or RollupConfig()
    vendor_year_expr = pl.lit(None, dtype=pl.Float64)
    for vendor, years in config.blending.vendor_years.items():
        vendor_year_expr = pl.when(pl.col(Col.vendor) == vendor).then(float(years)).otherwise(vendor_year_expr)
    bucket_expr = pl.lit(0)
    for point in sorted(
        (p for p in config.blending.target_points if p.ep_type == "OEP"),
        key=lambda p: p.return_period,
    ):
        bucket_expr = pl.when(pl.col(Col.rp) >= point.return_period).then(point.return_period).otherwise(bucket_expr)
    return ylt.with_columns(
        pl.col(Col.loss)
        .rank(method="ordinal", descending=True)
        .over(Col.vendor, Col.modelled_lob, Col.rollup_peril)
        .cast(pl.Int64)
        .alias(Col.rnk)
    ).with_columns(
        (vendor_year_expr / pl.col(Col.rnk))
        .alias(Col.rp)
    ).with_columns(
        bucket_expr.alias(Col.rp_bucket)
    )


def apply_ep_blending_to_ylt(
    ylt: pl.LazyFrame,
    ep_blending_targets: EpBlendingTargets,
) -> pl.LazyFrame:
    factors = ep_blending_targets.blended.select(
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.blend_subregion_peril_id,
        pl.col(Col.return_period).alias(Col.rp_bucket),
        Col.ep_type,
        Col.risklink_loss,
        Col.verisk_loss,
        Col.risklink_blended_contribution,
        Col.verisk_blended_contribution,
        Col.target_loss,
        Col.base_model,
        Col.base_model_loss,
        Col.uplift_factor_on_base_model,
    )

    ylt_cols = ylt.collect_schema().names()
    diagnostic_cols = [
        Col.risklink_blended_contribution,
        Col.verisk_blended_contribution,
        Col.uplift_factor_on_base_model,
    ]
    return (
        ylt.join(
            factors,
            on=[
                Col.rollup_lob,
                Col.rollup_peril,
                Col.region_peril_id,
                Col.blend_subregion_peril_id,
                Col.rp_bucket,
                Col.base_model,
            ],
            how="inner",
        )
        .with_columns(
            (pl.col(Col.loss) * pl.col(Col.uplift_factor_on_base_model)).alias(Col.loss),
            pl.lit("blended").alias(Col.metric),
        )
        .select([*ylt_cols, *diagnostic_cols])
    )


def apply_fx_to_ylt(
    blended: pl.LazyFrame,
    seeds: SeedValidationResult,
) -> pl.LazyFrame:
    fx_rates = (
        seeds.frames["fx_rates.csv"]
        .lazy()
        .filter(pl.col(Col.target_currency) == "GBP")
        .select(
            pl.col(RawCol.currency_code).alias(Col.currency),
            Col.target_currency,
            pl.col(RawCol.rate_date).alias(Col.fx_rate_date),
            pl.col(RawCol.rate).alias(Col.fx_rate),
        )
    )

    blended_cols = blended.collect_schema().names()
    return (
        blended.join(fx_rates, on=Col.currency, how="inner")
        .with_columns(
            (pl.col(Col.loss) * pl.col(Col.fx_rate)).alias(Col.loss),
            pl.lit("gbp").alias(Col.metric),
        )
        .select([*blended_cols, Col.target_currency])
    )


def apply_forecast_to_ylt(
    gbp: pl.LazyFrame,
    seeds: SeedValidationResult,
) -> pl.LazyFrame:
    forecast_factors = seeds.frames["forecast_factors.csv"].lazy()
    forecast_dates = forecast_factors.select(Col.forecast_date).unique()
    forecast_factors = forecast_factors.select(
        Col.class_,
        pl.col("office_iso2").alias(Col.office),
        Col.forecast_date,
        pl.col(RawCol.factor).alias("_forecast_factor_raw"),
    )

    forecasted = (
        gbp.join(forecast_dates, how="cross")
        .join(
            forecast_factors,
            on=[Col.class_, Col.office, Col.forecast_date],
            how="left",
        )
    )
    _log_defaulted_rows(
        forecasted,
        pl.col("_forecast_factor_raw").is_null(),
        "forecast factor defaulted rows=%d",
    )
    return (
        forecasted
        .with_columns(
            (pl.col(Col.loss) * pl.col("_forecast_factor_raw").fill_null(1.0)).alias(Col.loss),
            pl.lit("gbp_forecast").alias(Col.metric),
        )
        .drop("_forecast_factor_raw")
    )


def apply_euws_to_ylt(
    gbp_forecast: pl.LazyFrame,
    verisk_events: pl.LazyFrame,
    seeds: SeedValidationResult,
) -> pl.LazyFrame:
    euws_factors = seeds.frames["euws_rate_factors.csv"].lazy().select(
        Col.model_event_id,
        pl.col(RawCol.factor).alias("_euws_factor_raw_source"),
    )

    gbp_forecast_cols = gbp_forecast.collect_schema().names()
    joined = (
        gbp_forecast.join(
            verisk_events,
            on=[Col.event_id, Col.year_id, Col.model_code],
            how="left",
        )
        .join(euws_factors, on=Col.model_event_id, how="left")
    )
    _log_defaulted_rows(
        joined,
        pl.col("_euws_factor_raw_source").is_null(),
        "euws factor defaulted rows=%d",
    )
    return (
        joined
        .with_columns(
            pl.col("_euws_factor_raw_source").fill_null(1.0).alias("_euws_factor_raw")
        )
        .with_columns(
            pl.col(Col.loss).alias("_gbp_forecast_loss"),
            (pl.col(Col.loss) * pl.col("_euws_factor_raw")).alias(Col.loss),
            pl.lit("euws").alias(Col.metric),
        )
        .drop("_euws_factor_raw_source")
        .select([*gbp_forecast_cols, "_euws_factor_raw", "_gbp_forecast_loss", Col.model_event_id, Col.event_day])
    )


def apply_euws_overrides_to_ylt(
    euws: pl.LazyFrame,
    seeds: SeedValidationResult,
) -> pl.LazyFrame:
    overrides = seeds.frames["euws_rank_overrides.csv"].lazy().select(
        Col.rollup_lob,
        pl.col(RawCol.max_rank).alias("_euws_override_max_rank"),
        pl.col(RawCol.factor).alias("_euws_override_factor"),
    )
    euws_cols = euws.collect_schema().names()
    override_condition = (
        pl.col("_euws_override_factor").is_not_null()
        & (pl.col(Col.rnk) <= pl.col("_euws_override_max_rank"))
        & (pl.col("_euws_factor_raw") == 0)
    )

    return (
        euws.join(overrides, on=Col.rollup_lob, how="left")
        .with_columns(
            pl.when(override_condition)
            .then(pl.col("_euws_override_factor"))
            .otherwise(pl.col("_euws_factor_raw"))
            .alias("_euws_factor")
        )
        .with_columns(
            pl.when(override_condition)
            .then(pl.col("_gbp_forecast_loss") * pl.col("_euws_override_factor"))
            .otherwise(pl.col(Col.loss))
            .alias(Col.loss),
            pl.lit("euws_override").alias(Col.metric),
        )
        .drop("_euws_override_max_rank", "_euws_override_factor", "_euws_factor")
        .select(euws_cols)
    )


def calculate_dialsup(
    ylt: pl.LazyFrame,
    verisk_events: pl.LazyFrame,
    seeds: SeedValidationResult,
) -> pl.LazyFrame:
    fx_rates = (
        seeds.frames["fx_rates.csv"]
        .lazy()
        .filter(pl.col(Col.target_currency) == "GBP")
        .select(
            pl.col(RawCol.currency_code).alias(Col.currency),
            Col.target_currency,
            pl.col(RawCol.rate_date).alias(Col.fx_rate_date),
            pl.col(RawCol.rate).alias(Col.fx_rate),
        )
    )
    forecast_factors = seeds.frames["forecast_factors.csv"].lazy()
    forecast_dates = forecast_factors.select(Col.forecast_date).unique()
    forecast_factors = forecast_factors.select(
        Col.class_,
        pl.col("office_iso2").alias(Col.office),
        Col.forecast_date,
        pl.col(RawCol.factor).alias("_forecast_factor_raw"),
    )

    base = (
        ylt.join(
            verisk_events,
            on=[Col.event_id, Col.year_id, Col.model_code],
            how="left",
        )
        .join(fx_rates, on=Col.currency, how="inner")
        .join(forecast_dates, how="cross")
        .join(
            forecast_factors,
            on=[Col.class_, Col.office, Col.forecast_date],
            how="left",
        )
        .with_columns(
            pl.col("_forecast_factor_raw").fill_null(1.0).alias("_forecast_factor"),
        )
    )
    _log_defaulted_rows(
        base,
        pl.col("_forecast_factor_raw").is_null(),
        "dialsup forecast factor defaulted rows=%d",
    )

    output_cols = [c for c in base.collect_schema().names() if c not in ("_forecast_factor_raw", "_forecast_factor")]

    dialsup_original = base.with_columns(
        pl.lit("dialsup_original").alias(Col.metric),
    ).select(output_cols)

    dialsup_gbp = base.with_columns(
        (pl.col(Col.loss) * pl.col(Col.fx_rate)).alias(Col.loss),
        pl.lit("dialsup_gbp").alias(Col.metric),
    ).select(output_cols)

    dialsup_gbp_forecast = base.with_columns(
        (pl.col(Col.loss) * pl.col(Col.fx_rate) * pl.col("_forecast_factor")).alias(Col.loss),
        pl.lit("dialsup_gbp_forecast").alias(Col.metric),
    ).select(output_cols)

    return pl.concat([dialsup_original, dialsup_gbp, dialsup_gbp_forecast])


def enrich_risklink_event_days(
    ylt: pl.LazyFrame,
    risklink_events: pl.LazyFrame,
) -> pl.LazyFrame:
    join_year = "__risklink_model_occurrence_year"
    non_risklink = ylt.filter(pl.col(Col.base_model) != "risklink").with_columns(
        pl.lit(None).cast(pl.Int64).alias(Col.model_occurrence_year),
        pl.lit(None).cast(pl.Int64).alias(Col.risklink_event_day),
    )
    risklink_before = ylt.filter(pl.col(Col.base_model) == "risklink")
    risklink = risklink_before.join(
        risklink_events.with_columns(pl.col(Col.model_occurrence_year).alias(join_year)),
        left_on=[Col.event_id, Col.year_id, Col.region_peril_id],
        right_on=[Col.event_id, join_year, Col.region_peril_id],
        how="inner",
    )
    before_count = risklink_before.select(pl.len()).collect().item()
    after_count = risklink.select(pl.len()).collect().item()
    logger.info(
        "risklink event-day join rows before=%d after=%d dropped=%d",
        before_count,
        after_count,
        before_count - after_count,
        extra={
            "event": "risklink_event_day_join",
            "before_rows": before_count,
            "after_rows": after_count,
            "dropped_rows": before_count - after_count,
        },
    )
    return pl.concat([non_risklink, risklink], how="diagonal_relaxed")


def build_main_fanout(
    ylt: pl.LazyFrame,
    risklink_events: pl.LazyFrame,
) -> pl.LazyFrame:
    ylt = enrich_risklink_event_days(ylt, risklink_events)
    return ylt.select(
        Col.forecast_date,
        Col.base_model,
        pl.col(Col.metric),
        pl.when(pl.col(Col.base_model) == "risklink")
        .then(pl.col(Col.event_id))
        .otherwise(pl.col(Col.model_event_id))
        .cast(pl.Int64)
        .alias(FanoutCol.ModelEventID),
        pl.col(Col.year_id).cast(pl.Int64).alias(FanoutCol.ModelYear),
        pl.col(Col.target_currency).alias(FanoutCol.CurrencyCode),
        pl.lit(0).cast(pl.Int64).alias(FanoutCol.ModelYOA),
        pl.col(Col.loss)
        .cast(pl.Float64)
        .alias(FanoutCol.ModelGrossLoss),
        pl.lit(0).cast(pl.Int64).alias(FanoutCol.ModelInwardsReinstatement),
        pl.when(pl.col(Col.base_model) == "risklink")
        .then(pl.col(Col.risklink_event_day))
        .otherwise(pl.col(Col.event_day))
        .cast(pl.Int64)
        .alias(FanoutCol.ModelEventDay),
        pl.col(Col.cds_cat_class_name).alias(FanoutCol.LossClassName),
    )


def build_dialsup_fanout(
    ylt: pl.LazyFrame,
    risklink_events: pl.LazyFrame,
) -> pl.LazyFrame:
    ylt = enrich_risklink_event_days(ylt, risklink_events)
    return ylt.select(
        Col.forecast_date,
        Col.base_model,
        pl.col(Col.metric),
        pl.when(pl.col(Col.base_model) == "risklink")
        .then(pl.col(Col.event_id))
        .otherwise(pl.col(Col.model_event_id))
        .cast(pl.Int64)
        .alias(FanoutCol.ModelEventID),
        pl.col(Col.year_id).cast(pl.Int64).alias(FanoutCol.ModelYear),
        pl.col(Col.target_currency).alias(FanoutCol.CurrencyCode),
        pl.lit(0).cast(pl.Int64).alias(FanoutCol.ModelYOA),
        pl.col(Col.loss)
        .cast(pl.Float64)
        .alias(FanoutCol.ModelGrossLoss),
        pl.lit(0).cast(pl.Int64).alias(FanoutCol.ModelInwardsReinstatement),
        pl.when(pl.col(Col.base_model) == "risklink")
        .then(pl.col(Col.risklink_event_day))
        .otherwise(pl.col(Col.event_day))
        .cast(pl.Int64)
        .alias(FanoutCol.ModelEventDay),
        pl.col(Col.cds_cat_class_name).alias(FanoutCol.LossClassName),
    )


def apply_event_loss_threshold(
    frame: pl.DataFrame | pl.LazyFrame,
    *,
    metric: str,
    threshold: float,
) -> pl.DataFrame | pl.LazyFrame:
    """Filter final metric rows by configured event loss threshold.

    Non-final metrics are preserved so historical/intermediate rows remain in
    the combined long outputs, while exported final rows and fanouts can share
    the same threshold semantics.
    """
    loss_is_kept = (
        pl.col(Col.loss).is_not_null()
        if threshold <= 0
        else pl.col(Col.loss) >= threshold
    )
    return frame.filter((pl.col(Col.metric) != metric) | loss_is_kept)


def build_event_validation_report(
    *fanouts: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame | pl.LazyFrame:
    reports = []
    for fanout in fanouts:
        reports.append(
            _as_lazy_frame(fanout).group_by(Col.base_model, Col.metric, Col.forecast_date).agg(
                pl.len().alias(Col.row_count),
                pl.col(FanoutCol.ModelEventID)
                .is_null()
                .sum()
                .alias(Col.missing_model_event_id),
                pl.col(FanoutCol.ModelEventDay)
                .is_null()
                .sum()
                .alias(Col.missing_model_event_day),
            )
        )
    return pl.concat(reports, how="vertical")


def _as_lazy_frame(frame: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    if isinstance(frame, pl.LazyFrame):
        return frame
    return frame.lazy()


_CDS_FANOUT_COLUMNS = [
    FanoutCol.ModelEventID,
    FanoutCol.ModelYear,
    FanoutCol.CurrencyCode,
    FanoutCol.ModelYOA,
    FanoutCol.ModelGrossLoss,
    FanoutCol.ModelInwardsReinstatement,
    FanoutCol.ModelEventDay,
    FanoutCol.LossClassName,
]


def _mts_output_dimensions(frame: pl.DataFrame | pl.LazyFrame) -> list[str]:
    diagnostic_cols = {
        Col.risklink_blended_contribution,
        Col.verisk_blended_contribution,
        Col.uplift_factor_on_base_model,
    }
    return [
        col for col in frame.columns
        if col not in (Col.metric, Col.forecast_date, Col.loss)
        and col not in diagnostic_cols
        and not col.startswith("_")
    ]


_MTS_WIDE_IDENTITY_COLUMN_ORDER = [
    Col.vendor,
    Col.analysis_id,
    Col.base_model,
    Col.output_use,
    Col.model_code,
    Col.year_id,
    Col.event_id,
    Col.model_event_id,
    Col.event_day,
    Col.risklink_event_day,
    Col.model_occurrence_year,
    Col.model_occurrence_date,
    Col.rollup_lob,
    Col.rollup_peril,
    Col.modelled_lob,
    Col.modelled_peril,
    Col.region_peril_id,
    Col.blend_subregion_peril_id,
    Col.sub_region_peril_id,
    Col.sub_region_peril,
    Col.cds_cat_class_name,
    Col.class_,
    Col.office,
    Col.currency,
    Col.target_currency,
    Col.fx_rate_date,
    Col.fx_rate,
    Col.rnk,
    Col.rp,
    Col.rp_bucket,
    Col.forecast_factor_raw,
    Col.forecast_factor,
    Col.euws_factor_raw_source,
    Col.euws_factor_raw,
    Col.euws_factor,
    Col.euws_override_max_rank,
    Col.euws_override_factor,
    Col.euws_override_applied,
]

_MTS_WIDE_BASE_LOSS_COLUMN_ORDER = [
    Col.original_ylt_loss,
    Col.risklink_loss,
    Col.verisk_loss,
    Col.base_model_loss,
    Col.target_loss,
]

_MTS_WIDE_BLEND_DIAGNOSTIC_COLUMN_ORDER = [
    Col.risklink_weight,
    Col.verisk_weight,
    Col.risklink_blended_contribution,
    Col.verisk_blended_contribution,
    Col.uplift_factor_on_base_model,
]

_MTS_WIDE_MAIN_LOSS_PREFIX_ORDER = [
    "original",
    Col.original_ylt_loss_blended,
    "blended",
    Col.original_ylt_loss_blended_gbp,
    "gbp",
    Col.original_ylt_loss_blended_gbp_forecast,
    "gbp_forecast",
    Col.original_ylt_loss_blended_gbp_forecast_euws_raw,
    Col.original_ylt_loss_blended_gbp_forecast_euws,
    "euws",
    "euws_override",
]


def _ordered_mts_wide_columns(columns: list[str]) -> list[str]:
    """Return the presentation order for mts_tbl_ylt_combined_all_factors_wide.

    The helper is intentionally presentation-only: it preserves the supplied
    column set exactly once while grouping columns into identity context, base
    loss context, blend diagnostics, main loss transforms, DIALSUP, and finally
    any unknown columns in their existing relative order.
    """
    remaining = list(dict.fromkeys(columns))
    ordered: list[str] = []

    def take_exact(priority: list[str]) -> None:
        for column in priority:
            if column in remaining:
                ordered.append(column)
                remaining.remove(column)

    def take_matching(predicate: Callable[[str], bool]) -> None:
        matched = [column for column in remaining if predicate(column)]
        ordered.extend(matched)
        for column in matched:
            remaining.remove(column)

    def is_dialsup_column(column: str) -> bool:
        return column.startswith("dialsup_")

    def main_loss_priority(column: str) -> tuple[int, str]:
        for index, prefix in enumerate(_MTS_WIDE_MAIN_LOSS_PREFIX_ORDER):
            if column.startswith(f"{prefix}_"):
                return index, column
        return len(_MTS_WIDE_MAIN_LOSS_PREFIX_ORDER), column

    base_loss_columns = set(_MTS_WIDE_BASE_LOSS_COLUMN_ORDER)
    blend_diagnostic_columns = set(_MTS_WIDE_BLEND_DIAGNOSTIC_COLUMN_ORDER)

    take_exact(_MTS_WIDE_IDENTITY_COLUMN_ORDER)
    take_matching(
        lambda column: not column.endswith("_loss")
        and column not in base_loss_columns
        and column not in blend_diagnostic_columns
        and not is_dialsup_column(column)
    )
    take_exact(_MTS_WIDE_BASE_LOSS_COLUMN_ORDER)
    take_exact(_MTS_WIDE_BLEND_DIAGNOSTIC_COLUMN_ORDER)

    main_loss_columns = [
        column
        for column in remaining
        if column.endswith("_loss")
        and column not in base_loss_columns
        and not is_dialsup_column(column)
    ]
    for column in sorted(main_loss_columns, key=main_loss_priority):
        ordered.append(column)
        remaining.remove(column)

    take_matching(is_dialsup_column)
    ordered.extend(remaining)
    return ordered


def _write_combined_outputs(
    output_root: Path,
    ylt: pl.DataFrame,
    ylt_dialsup: pl.DataFrame,
) -> None:
    write_parquet_with_log(
        _with_metric_output_use(ylt, final_metric="euws_override", final_output_use="cds_main"),
        output_root / "mts_tbl_ylt_combined_all_factors.parquet",
    )
    write_parquet_with_log(
        _with_metric_output_use(
            ylt_dialsup.filter(pl.col(Col.metric) == "dialsup_gbp_forecast"),
            final_metric="dialsup_gbp_forecast",
            final_output_use="cds_dialsup",
        ),
        output_root / "mts_tbl_ylt_dialsup.parquet",
    )

    dims = _mts_output_dimensions(ylt)
    diagnostic_cols = [
        Col.risklink_blended_contribution,
        Col.verisk_blended_contribution,
        Col.uplift_factor_on_base_model,
    ]
    sel = [*dims, Col.metric, Col.forecast_date, Col.loss]
    dialsup_sel = [
        pl.col(col) if col in ylt_dialsup.columns else pl.lit(None).alias(col)
        for col in sel
    ]

    combined = pl.concat(
        [ylt.filter(pl.col(Col.metric) == "euws_override").select(sel),
         ylt_dialsup.filter(pl.col(Col.metric) == "dialsup_gbp_forecast").select(dialsup_sel)],
    ).with_columns(
        pl.concat_str(
            [pl.col(Col.metric), pl.col(Col.forecast_date).cast(pl.String).str.replace("-", "").str.slice(0, 6), pl.lit("loss")],
            separator="_",
        ).alias("_wide_column"),
    )

    wide = combined.pivot(
        index=dims,
        on="_wide_column",
        values=Col.loss,
        aggregate_function="first",
    )

    diagnostics = ylt.filter(pl.col(Col.metric) == "euws_override").select(
        [*dims, *[col for col in diagnostic_cols if col in ylt.columns]]
    ).unique(subset=dims, keep="first")
    if len(diagnostics.columns) > len(dims):
        wide = wide.join(diagnostics, on=dims, how="left")

    wide = wide.with_columns(pl.lit("cds_wide_analysis").alias(Col.output_use))
    wide = wide.select(_ordered_mts_wide_columns(wide.columns))

    write_parquet_with_log(wide, output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet")


def _with_metric_output_use(
    frame: pl.DataFrame,
    *,
    final_metric: str,
    final_output_use: str,
) -> pl.DataFrame:
    return frame.with_columns(
        pl.when(pl.col(Col.metric) == final_metric)
        .then(pl.lit(final_output_use))
        .otherwise(pl.lit("intermediate_audit"))
        .alias(Col.output_use)
    )


def write_debug_frame(
    debug_dir: Path,
    name: str,
    frame: pl.DataFrame | pl.LazyFrame,
) -> None:
    debug_dir.mkdir(parents=True, exist_ok=True)
    output_path = debug_dir / f"{name}.parquet"
    write_parquet_with_log(frame, output_path)


def write_parquet_with_log(frame: pl.DataFrame | pl.LazyFrame, output_path: Path) -> None:
    started = time.perf_counter()
    lazy = isinstance(frame, pl.LazyFrame)
    if isinstance(frame, pl.LazyFrame):
        frame.sink_parquet(output_path, mkdir=True)
        row_count = -1
    else:
        frame.write_parquet(output_path)
        row_count = frame.height
    elapsed_seconds = time.perf_counter() - started
    logger.info(
        "wrote output=%s rows=%d elapsed=%.2fs",
        output_path,
        row_count,
        elapsed_seconds,
        extra={
            "event": "write_output",
            "path": output_path,
            "rows": row_count,
            "elapsed_seconds": elapsed_seconds,
            "lazy": lazy,
        },
    )


def _log_checkpoint(name: str, frame: pl.DataFrame) -> None:
    logger.info(
        "checkpoint=%s rows=%d",
        name,
        frame.height,
        extra={"event": "checkpoint", "checkpoint": name, "rows": frame.height},
    )


def _warn_row_drop(name: str, before: int, after: int) -> None:
    if after < before:
        logger.warning(
            "%s join dropped rows before=%d after=%d dropped=%d",
            name,
            before,
            after,
            before - after,
            extra={
                "event": "row_drop",
                "join": name,
                "before_rows": before,
                "after_rows": after,
                "dropped_rows": before - after,
            },
        )


def _log_defaulted_rows(frame: pl.LazyFrame, condition: pl.Expr, message: str) -> None:
    defaulted_rows = frame.select(condition.sum().alias("defaulted_rows")).collect().item()
    if defaulted_rows > 0:
        logger.warning(message, defaulted_rows, extra={"event": "defaulted_rows", "rows": defaulted_rows})


def write_mart_outputs(output_root: Path, result: PipelineRunResult) -> None:
    output_dir = output_root / "marts"
    output_dir.mkdir(parents=True, exist_ok=True)

    fanouts: dict[str, pl.LazyFrame] = {}
    for name, frame in result.marts.frames.items():
        if not name.endswith("fanout"):
            continue
        fanouts[name] = _as_lazy_frame(frame)

    ylt_long = result.marts.frames.get("ylt_long")
    ylt_dialsup = result.marts.frames.get("ylt_dialsup")
    if ylt_long is not None and ylt_dialsup is not None:
        _write_combined_outputs(output_root, ylt_long, ylt_dialsup)

    if fanouts:
        write_parquet_with_log(
            build_event_validation_report(*fanouts.values()),
            output_root / "mts_event_validation.parquet",
        )

    for name, frame in fanouts.items():
        started = time.perf_counter()
        with tempfile.TemporaryDirectory(prefix=f"rollup-{name}-") as temp_dir:
            materialized_path = Path(temp_dir) / f"{name}.parquet"
            logger.info(
                "materializing fanout once fanout=%s output=%s",
                name,
                materialized_path,
                extra={"event": "fanout_materialize_start", "fanout": name, "path": materialized_path},
            )
            frame.sink_parquet(materialized_path)
            materialized = pl.scan_parquet(materialized_path)
            elapsed_seconds = time.perf_counter() - started
            logger.info(
                "materialized fanout fanout=%s elapsed=%.2fs",
                name,
                elapsed_seconds,
                extra={"event": "fanout_materialize_done", "fanout": name, "elapsed_seconds": elapsed_seconds},
            )
            _write_fanout_partitions(name, materialized, output_dir, started)


def _write_fanout_partitions(
    name: str,
    frame: pl.LazyFrame,
    output_dir: Path,
    started: float,
) -> None:
    logger.info("collecting fanout partitions fanout=%s", name, extra={"event": "fanout_partitions_start", "fanout": name})
    partitions = (
        frame.select(Col.forecast_date, Col.base_model, Col.metric)
        .unique()
        .sort(Col.forecast_date, Col.base_model, Col.metric)
        .collect()
    )
    elapsed_seconds = time.perf_counter() - started
    logger.info(
        "collected fanout partitions fanout=%s partitions=%d elapsed=%.2fs",
        name,
        partitions.height,
        elapsed_seconds,
        extra={
            "event": "fanout_partitions_done",
            "fanout": name,
            "partition_count": partitions.height,
            "elapsed_seconds": elapsed_seconds,
        },
    )
    for row in partitions.iter_rows(named=True):
        tag = forecast_tag(row[Col.forecast_date])
        vendor = hisco_vendor_label(row[Col.base_model])
        metric = row[Col.metric]
        output_path = output_dir / f"Hisco{vendor}_{tag}_{metric}.parquet"
        logger.info(
            "writing fanout partition fanout=%s output=%s base_model=%s metric=%s forecast_date=%s",
            name,
            output_path,
            row[Col.base_model],
            metric,
            row[Col.forecast_date],
            extra={
                "event": "fanout_partition_write",
                "fanout": name,
                "path": output_path,
                "base_model": row[Col.base_model],
                "metric": metric,
                "forecast_date": row[Col.forecast_date],
            },
        )
        write_parquet_with_log(
            frame.filter(
                (pl.col(Col.forecast_date) == row[Col.forecast_date])
                & (pl.col(Col.base_model) == row[Col.base_model])
                & (pl.col(Col.metric) == metric)
            ).select(_CDS_FANOUT_COLUMNS),
            output_path,
        )


def write_debug_outputs(output_root: Path, result: PipelineRunResult) -> None:
    debug_dir = output_root / "debug"
    for prefix, stage in {
        "seed": result.seeds,
        "stg": result.staging,
        "int": result.intermediate,
        "mts": result.marts,
    }.items():
        for name, frame in stage.frames.items():
            write_debug_frame(debug_dir, f"{prefix}_{name}", frame)


def forecast_tag(value: object) -> str:
    if hasattr(value, "strftime"):
        return value.strftime("%Y%m")
    return str(value).replace("-", "")[:6]


def hisco_vendor_label(base_model: str) -> str:
    if base_model == "verisk":
        return "AIR"
    if base_model == "risklink":
        return "RMS"
    raise ValueError(f"unknown base model: {base_model}")


def run(
    data_root: Path | str = "data",
    *,
    output_root: Path | str = "output",
    debug: bool = False,
    config: RollupConfig | None = None,
    validation_inputs: PipelineValidationInputs | None = None,
) -> PipelineRunResult:
    data_root = Path(data_root)
    output_root = Path(output_root)
    config = config or RollupConfig()
    seed_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {}
    staging_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {}
    intermediate_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {}
    mart_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {}

    with logged_phase("validation"):
        if validation_inputs is None:
            validation_inputs = load_pipeline_validation_inputs(data_root)
        ensure_pipeline_validation_inputs(validation_inputs)

        seeds = validation_inputs.seeds
        ylts = validation_inputs.ylts
        ep_summaries = validation_inputs.ep_summaries
        coverage_report = validation_inputs.coverage_report
        logger.info(
            "validation summary seed_files=%d ylt_files=%d ep_summary_files=%d coverage_errors=%d",
            seeds.report.height,
            ylts.report.height,
            ep_summaries.report.height,
            coverage_report.filter(pl.col("severity") == "error").height,
            extra={
                "event": "validation_summary",
                "seed_files": seeds.report.height,
                "ylt_files": ylts.report.height,
                "ep_summary_files": ep_summaries.report.height,
                "coverage_errors": coverage_report.filter(pl.col("severity") == "error").height,
            },
        )

    with logged_phase("staging"):
        for filename, frame in seeds.frames.items():
            seed_frames[Path(filename).stem] = frame
        verisk_events = load_verisk_events(data_root, config)
        seed_frames["verisk_events"] = verisk_events
        risklink_events = load_risklink_flood_events(data_root, config)
        seed_frames["risklink_flood_events"] = risklink_events
        staging_frames["validation_seeds"] = seeds.report

        staging_frames["validation_ylt"] = ylts.report
        staging_frames["modelled_dimension_coverage"] = coverage_report

        normalized_ylts = normalize_ylt(ylts)
        staging_frames["ylt_verisk_normalized"] = normalized_ylts.verisk
        staging_frames["ylt_risklink_normalized"] = normalized_ylts.risklink

        staging_frames["validation_ep_summaries"] = ep_summaries.report
        staging_frames["ep_summaries"] = ep_summaries.frame

        staged_ep_summaries = stage_ep_summaries(ep_summaries, seeds)
        staging_frames["ep_summaries_enriched"] = staged_ep_summaries.enriched
        staging_frames["ep_summaries_selected"] = staged_ep_summaries.selected
        staging_frames["ep_summaries_selected_dialsup"] = staged_ep_summaries.selected_dialsup
        logger.info(
            "staging summary seed_frames=%d staging_frames=%d",
            len(seed_frames),
            len(staging_frames),
            extra={"event": "staging_summary", "seed_frames": len(seed_frames), "staging_frames": len(staging_frames)},
        )

    with logged_phase("intermediate"):
        enriched_ylts = enrich_ylt_with_ep_summaries(normalized_ylts, staged_ep_summaries)
        enriched_ylts_dialsup = enrich_ylt_with_ep_summaries(
            normalized_ylts,
            staged_ep_summaries,
            use_dialsup_selection=True,
        )
        intermediate_frames["ylt_verisk_enriched"] = enriched_ylts.verisk
        intermediate_frames["ylt_risklink_enriched"] = enriched_ylts.risklink
        intermediate_frames["ylt_combined_enriched"] = enriched_ylts.combined
        intermediate_frames["ylt_combined_enriched_dialsup"] = enriched_ylts_dialsup.combined

        joined_ep_summaries = join_ep_summaries(staged_ep_summaries)
        intermediate_frames["ep_summaries_enriched"] = joined_ep_summaries.enriched
        intermediate_frames["ep_summaries_verisk"] = joined_ep_summaries.verisk
        intermediate_frames["ep_summaries_risklink"] = joined_ep_summaries.risklink
        intermediate_frames["ep_vendor_joined"] = joined_ep_summaries.joined

        ep_blending_targets = calculate_ep_blending_targets(joined_ep_summaries, seeds, config)
        intermediate_frames["ep_blending_target_points"] = ep_blending_targets.target_points
        intermediate_frames["ep_blending_weights"] = ep_blending_targets.weights
        intermediate_frames["ep_blending_targets"] = ep_blending_targets.blended

        ylt_original = enriched_ylts.combined.with_columns(
            pl.lit("original").alias(Col.metric),
        ).filter(pl.col(Col.vendor) == pl.col(Col.base_model))
        intermediate_frames["ylt_original"] = ylt_original

        ylt_ranked = _add_rank_columns(ylt_original, config)
        intermediate_frames["ylt_ranked"] = ylt_ranked
        ylt_ranked = ylt_ranked.collect()
        _log_checkpoint("ylt_original", ylt_ranked)
        _log_checkpoint("ylt_ranked", ylt_ranked)

        ylt_blended = apply_ep_blending_to_ylt(ylt_ranked.lazy(), ep_blending_targets)
        intermediate_frames["ylt_blending_applied"] = ylt_blended
        ylt_blended = ylt_blended.collect()
        _warn_row_drop("blending", ylt_ranked.height, ylt_blended.height)
        _log_checkpoint("ylt_blended", ylt_blended)

        ylt_original_dialsup = enriched_ylts_dialsup.combined.with_columns(
            pl.lit("original").alias(Col.metric),
        ).filter(pl.col(Col.vendor) == pl.col(Col.base_model))
        intermediate_frames["ylt_original_dialsup"] = ylt_original_dialsup
        ylt_ranked_dialsup = _add_rank_columns(ylt_original_dialsup, config)
        intermediate_frames["ylt_ranked_dialsup"] = ylt_ranked_dialsup
        ylt_ranked_dialsup = ylt_ranked_dialsup.collect()
        _log_checkpoint("ylt_original_dialsup", ylt_ranked_dialsup)
        _log_checkpoint("ylt_ranked_dialsup", ylt_ranked_dialsup)

        ylt_dialsup = calculate_dialsup(ylt_ranked_dialsup.lazy(), verisk_events, seeds)
        intermediate_frames["ylt_dialsup"] = ylt_dialsup
        ylt_dialsup = ylt_dialsup.collect()
        _log_checkpoint("ylt_dialsup", ylt_dialsup)

        ylt_gbp = apply_fx_to_ylt(ylt_blended.lazy(), seeds)
        intermediate_frames["ylt_fx_applied"] = ylt_gbp
        ylt_gbp = ylt_gbp.collect()
        _warn_row_drop("fx", ylt_blended.height, ylt_gbp.height)
        _log_checkpoint("ylt_gbp", ylt_gbp)

        ylt_gbp_forecast = apply_forecast_to_ylt(ylt_gbp.lazy(), seeds)
        intermediate_frames["ylt_forecast_applied"] = ylt_gbp_forecast
        ylt_gbp_forecast = ylt_gbp_forecast.collect()
        _log_checkpoint("ylt_gbp_forecast", ylt_gbp_forecast)

        ylt_euws = apply_euws_to_ylt(ylt_gbp_forecast.lazy(), verisk_events, seeds)
        intermediate_frames["ylt_euws_applied"] = ylt_euws
        ylt_euws = ylt_euws.collect()
        _log_checkpoint("ylt_euws", ylt_euws)

        ylt_euws_override = apply_euws_overrides_to_ylt(ylt_euws.lazy(), seeds)
        intermediate_frames["ylt_euws_override_applied"] = ylt_euws_override
        ylt_euws_override = ylt_euws_override.collect()
        _log_checkpoint("ylt_euws_override", ylt_euws_override)

        ylt = pl.concat(
            [ylt_ranked, ylt_blended, ylt_gbp, ylt_gbp_forecast, ylt_euws, ylt_euws_override],
            how="diagonal",
        )
        logger.info(
            "intermediate summary frames=%d",
            len(intermediate_frames),
            extra={"event": "intermediate_summary", "frames": len(intermediate_frames)},
        )

    with logged_phase("marts"):
        threshold = config.outputs.minimum_event_loss_threshold
        ylt_thresholded = apply_event_loss_threshold(
            ylt,
            metric="euws_override",
            threshold=threshold,
        )
        ylt_dialsup_thresholded = apply_event_loss_threshold(
            ylt_dialsup,
            metric="dialsup_gbp_forecast",
            threshold=threshold,
        )
        main_fanout = build_main_fanout(
            ylt_thresholded.lazy().filter(pl.col(Col.metric) == "euws_override"),
            risklink_events,
        )
        mart_frames["main_fanout"] = main_fanout

        dialsup_fanout = build_dialsup_fanout(
            ylt_dialsup_thresholded.lazy().filter(pl.col(Col.metric) == "dialsup_gbp_forecast"),
            risklink_events,
        )
        mart_frames["dialsup_fanout"] = dialsup_fanout

        mart_frames["event_validation"] = build_event_validation_report(
            main_fanout,
            dialsup_fanout,
        )
        mart_frames["ylt_long"] = ylt_thresholded
        mart_frames["ylt_dialsup"] = ylt_dialsup_thresholded

    result = PipelineRunResult(
        seeds=PipelineStage(seed_frames),
        staging=PipelineStage(staging_frames),
        intermediate=PipelineStage(intermediate_frames),
        marts=PipelineStage(mart_frames),
    )

    if debug:
        with logged_phase("debug_outputs"):
            write_debug_outputs(output_root, result)

    with logged_phase("write_outputs"):
        write_mart_outputs(output_root, result)

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
