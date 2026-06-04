from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pandera.polars as pa
import polars as pl
import yaml

from rollup.columns import Col, FanoutCol, RawCol


logger = logging.getLogger(__name__)


@contextmanager
def logged_phase(phase: str) -> Iterator[None]:
    started = time.perf_counter()
    logger.info("start phase=%s", phase)
    try:
        yield
    except Exception:
        logger.exception("failed phase=%s elapsed=%.2fs", phase, time.perf_counter() - started)
        raise
    logger.info("done phase=%s elapsed=%.2fs", phase, time.perf_counter() - started)


_DTYPE_MAP: dict[str, pl.DataType] = {
    "bool": pl.Boolean,
    "boolean": pl.Boolean,
    "date": pl.Date,
    "datetime": pl.Datetime("us"),
    "float32": pl.Float32,
    "float64": pl.Float64,
    "int32": pl.Int32,
    "int64": pl.Int64,
    "str": pl.String,
    "string": pl.String,
    "uint32": pl.UInt32,
    "uint64": pl.UInt64,
}


@dataclass(frozen=True)
class SeedValidationResult:
    frames: dict[str, pl.DataFrame]
    report: pl.DataFrame


def load_yaml_file_schemas(data_root: Path | str = "data") -> dict[str, pl.Schema]:
    data_root = Path(data_root)
    schemas: dict[str, pl.Schema] = {}

    for schema_path in sorted(data_root.rglob("schema.yaml")):
        with schema_path.open("r", encoding="utf-8") as file:
            payload = yaml.safe_load(file) or {}

        for dataset in (payload.get("datasets") or {}).values():
            path = dataset.get("path")
            if not path:
                continue

            filename = Path(path).name
            schemas[filename] = pl.Schema(
                {
                    column["name"]: _DTYPE_MAP[column["dtype"].lower()]
                    for column in dataset.get("columns") or []
                }
            )

    return schemas


def load_yaml_dataset_schemas(data_root: Path | str = "data") -> dict[str, pl.Schema]:
    data_root = Path(data_root)
    schemas: dict[str, pl.Schema] = {}

    for schema_path in sorted(data_root.rglob("schema.yaml")):
        with schema_path.open("r", encoding="utf-8") as file:
            payload = yaml.safe_load(file) or {}

        for dataset_name, dataset in (payload.get("datasets") or {}).items():
            schemas[dataset_name] = pl.Schema(
                {
                    column["name"]: _DTYPE_MAP[column["dtype"].lower()]
                    for column in dataset.get("columns") or []
                }
            )

    return schemas


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


def validate_lazyframe_schema(
    frame: pl.LazyFrame,
    expected_schema: pl.Schema,
) -> list[str]:
    actual_schema = frame.collect_schema()

    missing = sorted(set(expected_schema) - set(actual_schema))
    extra = sorted(set(actual_schema) - set(expected_schema))
    mismatches = [
        f"{column}: expected {expected_schema[column]}, got {actual_schema[column]}"
        for column in expected_schema
        if column in actual_schema and actual_schema[column] != expected_schema[column]
    ]

    errors: list[str] = []
    if missing:
        errors.append(f"missing columns: {missing}")
    if extra:
        errors.append(f"unexpected columns: {extra}")
    if mismatches:
        errors.append(f"dtype mismatches: {mismatches}")

    return errors


def load_validated_ylt_frames(data_root: Path | str = "data") -> YltValidationResult:
    data_root = Path(data_root)
    schemas = load_yaml_dataset_schemas(data_root)
    vendor_specs = {
        "verisk": ("raw_verisk_ylt", data_root / "ylt" / "verisk"),
        "risklink": ("raw_risklink_ylt", data_root / "ylt" / "risklink"),
    }

    frames: dict[str, pl.LazyFrame] = {}
    rows: list[dict[str, object]] = []

    for vendor, (schema_name, folder) in vendor_specs.items():
        expected_schema = schemas[schema_name]
        paths = sorted(folder.glob("*.parquet"))

        if not paths:
            frames[vendor] = pl.LazyFrame()
            rows.append(
                {
                    "vendor": vendor,
                    "filename": None,
                    "path": str(folder),
                    "valid": False,
                    "row_count": None,
                    "error": "no parquet files found",
                }
            )
            continue

        frames[vendor] = pl.scan_parquet(str(folder / "*.parquet"))

        for file_path in paths:
            file_frame = pl.scan_parquet(file_path)
            errors = validate_lazyframe_schema(file_frame, expected_schema)
            row_count = None

            if not errors:
                row_count = file_frame.select(pl.len().alias(Col.row_count)).collect().item()

            rows.append(
                {
                    "vendor": vendor,
                    "filename": file_path.name,
                    "path": str(file_path),
                    "valid": not errors,
                    "row_count": row_count,
                    "error": "; ".join(errors) if errors else None,
                }
            )

    return YltValidationResult(
        frames=YltFrames(
            verisk=frames["verisk"],
            risklink=frames["risklink"],
        ),
        report=pl.DataFrame(rows),
    )


def ylt_loss_validation_summary(data_root: Path | str = "data") -> pl.DataFrame:
    data_root = Path(data_root)
    vendor_specs = {
        "verisk": (data_root / "ylt" / "verisk", RawCol.GroundUpLoss, 10_000),
        "risklink": (data_root / "ylt" / "risklink", RawCol.loss, 100_000),
    }
    rows: list[dict[str, object]] = []

    for vendor, (folder, loss_column, divisor) in vendor_specs.items():
        for file_path in sorted(folder.glob("*.parquet")):
            frame = pl.scan_parquet(file_path)
            loss_sum = frame.select(pl.col(loss_column).sum().alias(Col.loss_sum)).collect().item()
            rows.append(
                {
                    "vendor": vendor,
                    "filename": file_path.name,
                    "path": str(file_path),
                    "loss_column": loss_column,
                    "divisor": divisor,
                    "loss_sum": loss_sum,
                    "scaled_loss": loss_sum / divisor,
                }
            )

    return pl.DataFrame(rows)


_MODELLED_DIMENSION_COVERAGE_SCHEMA = {
    "filename": pl.String,
    "path": pl.String,
    "check_name": pl.String,
    "severity": pl.String,
    "valid": pl.Boolean,
    "direction": pl.String,
    "source_group": pl.String,
    "dimension": pl.String,
    "field": pl.String,
    "value": pl.String,
    "count": pl.Int64,
    "row_count": pl.Int64,
    "error": pl.String,
    "message": pl.String,
}


def empty_modelled_dimension_coverage_report() -> pl.DataFrame:
    return pl.DataFrame(schema=_MODELLED_DIMENSION_COVERAGE_SCHEMA)


def _unique_values(
    frame: pl.DataFrame | pl.LazyFrame,
    column: str,
    *,
    alias: str = "value",
) -> pl.LazyFrame:
    lazy = frame.lazy() if isinstance(frame, pl.DataFrame) else frame
    return (
        lazy.select(pl.col(column).cast(pl.String).alias(alias))
        .filter(pl.col(alias).is_not_null())
        .group_by(alias)
        .agg(pl.len().alias("count"))
    )


def _coverage_rows(
    missing_values: pl.LazyFrame,
    *,
    severity: str,
    direction: str,
    source_group: str,
    dimension: str,
    field: str,
    path: Path,
    message: str,
) -> pl.LazyFrame:
    return missing_values.with_columns(
        pl.lit(None).cast(pl.String).alias("filename"),
        pl.lit(str(path)).alias("path"),
        pl.lit("modelled_dimension_coverage").alias("check_name"),
        pl.lit(severity).alias("severity"),
        pl.lit(False).alias("valid"),
        pl.lit(direction).alias("direction"),
        pl.lit(source_group).alias("source_group"),
        pl.lit(dimension).alias("dimension"),
        pl.lit(field).alias("field"),
        pl.col("count").cast(pl.Int64).alias("row_count"),
        pl.concat_str(
            [
                pl.lit(message),
                pl.lit(" field="),
                pl.lit(field),
                pl.lit(" value="),
                pl.col("value"),
                pl.lit(" count="),
                pl.col("count").cast(pl.String),
                pl.lit(" path="),
                pl.lit(str(path)),
            ]
        ).alias("error"),
        pl.lit(message).alias("message"),
    ).select(*_MODELLED_DIMENSION_COVERAGE_SCHEMA)


def modelled_dimension_coverage_report(
    seeds: SeedValidationResult,
    ylts: YltValidationResult,
    ep_summaries: EpSummaryValidationResult,
    data_root: Path | str = "data",
) -> pl.DataFrame:
    data_root = Path(data_root)
    structural_report = pl.concat(
        [seeds.report, ylts.report, ep_summaries.report],
        how="diagonal_relaxed",
    )
    if structural_report.filter(~pl.col("valid")).height:
        return empty_modelled_dimension_coverage_report()

    if "lobs.csv" not in seeds.frames or "perils.csv" not in seeds.frames:
        return empty_modelled_dimension_coverage_report()

    seed_lobs = _unique_values(seeds.frames["lobs.csv"], Col.modelled_lob)
    seed_perils = _unique_values(seeds.frames["perils.csv"], Col.modelled_peril)

    stc_verisk = ylts.frames.verisk.filter(pl.col(RawCol.CatalogTypeCode) == "STC")
    verisk_lobs = _unique_values(stc_verisk, RawCol.ExposureAttribute)
    verisk_perils = _unique_values(stc_verisk, RawCol.Analysis)
    ep_lobs = _unique_values(ep_summaries.frame, Col.modelled_lob)
    ep_perils = _unique_values(ep_summaries.frame, Col.modelled_peril)

    report_parts = [
        _coverage_rows(
            verisk_lobs.join(seed_lobs, on="value", how="anti"),
            severity="error",
            direction="input_missing_from_seed",
            source_group="verisk_ylt",
            dimension=Col.modelled_lob,
            field=RawCol.ExposureAttribute,
            path=data_root / "ylt" / "verisk" / "*.parquet",
            message="Verisk YLT modelled LOB is missing from lobs.csv.",
        ),
        _coverage_rows(
            verisk_perils.join(seed_perils, on="value", how="anti"),
            severity="error",
            direction="input_missing_from_seed",
            source_group="verisk_ylt",
            dimension=Col.modelled_peril,
            field=RawCol.Analysis,
            path=data_root / "ylt" / "verisk" / "*.parquet",
            message="Verisk YLT modelled peril is missing from perils.csv.",
        ),
        _coverage_rows(
            ep_lobs.join(seed_lobs, on="value", how="anti"),
            severity="error",
            direction="input_missing_from_seed",
            source_group="ep_summaries",
            dimension=Col.modelled_lob,
            field=Col.modelled_lob,
            path=data_root / "ep_summaries" / "**" / "*.long.csv",
            message="EP summary modelled LOB is missing from lobs.csv.",
        ),
        _coverage_rows(
            ep_perils.join(seed_perils, on="value", how="anti"),
            severity="error",
            direction="input_missing_from_seed",
            source_group="ep_summaries",
            dimension=Col.modelled_peril,
            field=Col.modelled_peril,
            path=data_root / "ep_summaries" / "**" / "*.long.csv",
            message="EP summary modelled peril is missing from perils.csv.",
        ),
    ]

    report = pl.concat(report_parts, how="vertical").collect()
    if report.is_empty():
        return empty_modelled_dimension_coverage_report()
    return report.sort(["severity", "direction", "source_group", "dimension", "value"])


def build_semantic_validation_report(
    seeds: SeedValidationResult,
    ylts: YltValidationResult,
    ep_summaries: EpSummaryValidationResult,
    data_root: Path | str = "data",
) -> pl.DataFrame:
    coverage = modelled_dimension_coverage_report(seeds, ylts, ep_summaries, data_root)
    structural_report = pl.concat(
        [seeds.report, ylts.report, ep_summaries.report],
        how="diagonal_relaxed",
    )
    if structural_report.filter(~pl.col("valid")).height:
        return coverage
    if coverage.filter(pl.col("severity") == "error").height:
        return coverage
    return pl.concat(
        [coverage, dialsup_peril_selection_report(seeds, ep_summaries, data_root)],
        how="vertical",
    )


def dialsup_peril_selection_report(
    seeds: SeedValidationResult,
    ep_summaries: EpSummaryValidationResult,
    data_root: Path | str = "data",
) -> pl.DataFrame:
    if "lobs.csv" not in seeds.frames or "perils.csv" not in seeds.frames:
        return empty_modelled_dimension_coverage_report()

    perils_frame = seeds.frames["perils.csv"]
    if Col.is_dialsup not in perils_frame.columns:
        return empty_modelled_dimension_coverage_report()

    data_root = Path(data_root)
    lobs = seeds.frames["lobs.csv"].lazy().select(Col.modelled_lob, Col.rollup_lob)
    perils = perils_frame.lazy().select(
        Col.modelled_peril,
        Col.rollup_peril,
        Col.is_dialsup,
    )
    selection_keys = [Col.vendor, Col.rollup_lob, Col.rollup_peril]
    invalid_groups = (
        ep_summaries.frame.join(lobs, on=Col.modelled_lob, how="inner")
        .join(perils, on=Col.modelled_peril, how="inner")
        .select(*selection_keys, Col.modelled_peril, Col.is_dialsup)
        .unique()
        .group_by(selection_keys)
        .agg(
            pl.col(Col.is_dialsup).sum().cast(pl.Int64).alias("count"),
            pl.col(Col.modelled_peril)
            .filter(pl.col(Col.is_dialsup) == 1)
            .sort()
            .first()
            .alias("value"),
        )
        .with_columns(pl.col("value").fill_null("<none>"))
        .filter(pl.col("count") != 1)
    )
    report = _coverage_rows(
        invalid_groups,
        severity="error",
        direction="invalid_seed_configuration",
        source_group="ep_summaries",
        dimension=Col.is_dialsup,
        field=Col.is_dialsup,
        path=data_root / "seeds" / "business" / "perils.csv",
        message="Active vendor/rollup_lob/rollup_peril group must have exactly one is_dialsup=1 candidate in perils.csv.",
    ).collect()
    if report.is_empty():
        return empty_modelled_dimension_coverage_report()
    return report.sort(["severity", "direction", "source_group", "dimension", "value"])


def ensure_modelled_dimension_coverage(report: pl.DataFrame) -> None:
    error_report = report.filter(pl.col("severity") == "error")
    error_count = error_report.height
    if error_count:
        if "dimension" in error_report.columns and Col.is_dialsup in error_report.get_column(
            "dimension"
        ).to_list():
            raise ValueError(
                f"DIALSUP peril selection validation failed for {error_count} active group(s); "
                "run `rollup validate` for details"
            )
        raise ValueError(
            f"modelled LOB/peril coverage validation failed for {error_count} value(s); "
            "run `rollup validate` for details"
        )


def ensure_valid_inputs(
    seeds: SeedValidationResult,
    ylts: YltValidationResult,
    ep_summaries: EpSummaryValidationResult,
) -> None:
    report = pl.concat(
        [
            seeds.report.with_columns(pl.lit("seeds").alias("source_group")),
            ylts.report.with_columns(pl.lit("ylt").alias("source_group")),
            ep_summaries.report.with_columns(pl.lit("ep_summaries").alias("source_group")),
        ],
        how="diagonal",
    )
    invalid_count = report.filter(~pl.col("valid")).height
    if invalid_count:
        raise ValueError(
            f"input validation failed for {invalid_count} file(s); run `rollup validate` for details"
        )


def load_verisk_events(data_root: Path | str = "data") -> pl.LazyFrame:
    return pl.scan_parquet(
        Path(data_root) / "seeds" / "validation" / "verisk_events.parquet"
    ).select(
        pl.col(RawCol.EventID).alias(Col.model_event_id),
        pl.col(RawCol.ModelID).alias(Col.model_code),
        pl.col(RawCol.Event).alias(Col.event_id),
        pl.col(RawCol.Year).alias(Col.year_id),
        pl.col(RawCol.Day).alias(Col.event_day),
    )


def load_risklink_flood_events(data_root: Path | str = "data") -> pl.LazyFrame:
    return (
        pl.scan_parquet(
            Path(data_root) / "seeds" / "validation" / "risklink_flood22_model_events.parquet"
        )
        .group_by(FanoutCol.ModelEventID, RawCol.RegionPerilID)
        .agg(pl.col(RawCol.ModelOccurrenceDate).min().alias(Col.model_occurrence_date))
        .select(
            pl.col(FanoutCol.ModelEventID).alias(Col.event_id),
            pl.col(RawCol.RegionPerilID).alias(Col.region_peril_id),
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
    schemas = load_yaml_dataset_schemas(data_root)
    expected_schema = schemas["canonical_ep_summary"]
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

    frame = pl.scan_csv(str(folder / "**" / "*.long.csv"), schema_overrides=expected_schema)
    rows: list[dict[str, object]] = []

    for file_path in paths:
        file_frame = pl.scan_csv(file_path, schema_overrides=expected_schema)
        errors = validate_lazyframe_schema(file_frame, expected_schema)
        row_count = None

        if not errors:
            row_count = file_frame.select(pl.len().alias(Col.row_count)).collect().item()

        rows.append(
            {
                "filename": file_path.name,
                "path": str(file_path),
                "valid": not errors,
                "row_count": row_count,
                "error": "; ".join(errors) if errors else None,
            }
        )

    return EpSummaryValidationResult(frame=frame, report=pl.DataFrame(rows))


def pandera_schema_from_polars_schema(schema: pl.Schema) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {name: pa.Column(dtype, nullable=True) for name, dtype in schema.items()},
        strict=True,
    )


def load_validated_seed_frames(data_root: Path | str = "data") -> SeedValidationResult:
    data_root = Path(data_root)
    schemas = load_yaml_file_schemas(data_root)

    frames: dict[str, pl.DataFrame] = {}
    rows: list[dict[str, object]] = []

    for file_path in sorted((data_root / "seeds").rglob("*.csv")):
        filename = file_path.name
        expected_schema = schemas.get(filename)

        if expected_schema is None:
            rows.append(
                {
                    "filename": filename,
                    "path": str(file_path),
                    "valid": False,
                    "row_count": None,
                    "error": "no schema found",
                }
            )
            continue

        try:
            frame = pl.read_csv(file_path, schema_overrides=expected_schema)
            validated = pandera_schema_from_polars_schema(expected_schema).validate(frame)
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


_INPUT_YLT_AAL_SUMMARY_SCHEMA = {
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


def empty_input_ylt_aal_by_lob_peril_summary() -> pl.DataFrame:
    return pl.DataFrame(schema=_INPUT_YLT_AAL_SUMMARY_SCHEMA)


def load_pipeline_validation_inputs(
    data_root: Path | str = "data",
) -> PipelineValidationInputs:
    data_root = Path(data_root)
    seeds = load_validated_seed_frames(data_root)
    ylts = load_validated_ylt_frames(data_root)
    ep_summaries = load_validated_ep_summary_frames(data_root)
    coverage_report = build_semantic_validation_report(
        seeds,
        ylts,
        ep_summaries,
        data_root,
    )
    return PipelineValidationInputs(
        seeds=seeds,
        ylts=ylts,
        ep_summaries=ep_summaries,
        coverage_report=coverage_report,
    )


def ensure_pipeline_validation_inputs(inputs: PipelineValidationInputs) -> None:
    ensure_valid_inputs(inputs.seeds, inputs.ylts, inputs.ep_summaries)
    ensure_modelled_dimension_coverage(inputs.coverage_report)


def normalize_ylt(ylt: YltValidationResult) -> NormalizedYltFrames:
    verisk = ylt.frames.verisk.filter(pl.col(RawCol.CatalogTypeCode) == "STC").select(
        pl.lit("verisk").alias(Col.vendor),
        pl.col(RawCol.Analysis).cast(pl.String).alias(Col.analysis_id),
        pl.col(RawCol.Analysis).cast(pl.String).alias(Col.modelled_peril),
        pl.col(RawCol.ExposureAttribute).cast(pl.String).alias(Col.modelled_lob),
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
        Col.selection_priority,
        Col.is_dialsup,
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
        inputs.ylts.frames.verisk.filter(pl.col(RawCol.CatalogTypeCode) == "STC")
        .select(
            pl.lit("verisk").alias(Col.vendor),
            pl.col(RawCol.ExposureAttribute).cast(pl.String).alias(Col.modelled_lob),
            pl.col(RawCol.Analysis).cast(pl.String).alias(Col.modelled_peril),
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
) -> EpBlendingTargets:
    target_points = joined_ep_summaries.joined.filter(
        ((pl.col(Col.ep_type) == "AAL") & (pl.col(Col.return_period) == 0))
        | (
            (pl.col(Col.ep_type) == "OEP")
            & (pl.col(Col.return_period).is_in([200, 1000]))
        )
    )

    weights = (
        seeds.frames["blending_factors.csv"]
        .lazy()
        .filter(
            (pl.col(RawCol.RegionPerilID) != 216)
            | (pl.col(RawCol.SubRegionPerilID) == "216b")
        )
        .sort(RawCol.SubRegionPerilID)
        .group_by(RawCol.RegionPerilID)
        .first()
        .select(
            pl.col(RawCol.RegionPerilID).alias(Col.region_peril_id),
            pl.col(RawCol.SubRegionPerilID).alias(Col.sub_region_peril_id),
            pl.col(RawCol.SubRegionPeril).alias(Col.sub_region_peril),
            pl.col(RawCol.AIRBlend).cast(pl.Float64).alias(Col.verisk_weight),
            pl.col(RawCol.RMSBlend).cast(pl.Float64).alias(Col.risklink_weight),
        )
    )

    blended = (
        target_points.filter(
            pl.col(Col.risklink_loss).is_not_null()
            & pl.col(Col.verisk_loss).is_not_null()
        )
        .join(weights, on=Col.region_peril_id, how="left")
        .with_columns(
            (
                (pl.col(Col.verisk_loss) * pl.col(Col.verisk_weight))
                + (pl.col(Col.risklink_loss) * pl.col(Col.risklink_weight))
            ).alias(Col.target_loss)
        )
        .with_columns(
            pl.when(pl.col(Col.rollup_peril).is_in(["Europe_FL", "UK_FL"]))
            .then(pl.lit("risklink"))
            .otherwise(pl.lit("verisk"))
            .alias(Col.base_model)
        )
        .with_columns(
            pl.when(pl.col(Col.base_model) == "risklink")
            .then(pl.col(Col.risklink_loss))
            .otherwise(pl.col(Col.verisk_loss))
            .alias(Col.base_model_loss)
        )
        .with_columns(
            (pl.col(Col.target_loss) / pl.col(Col.base_model_loss)).alias(
                Col.uplift_factor_on_base_model
            )
        )
        .with_columns(
            pl.col(Col.uplift_factor_on_base_model)
            .clip(lower_bound=0.1, upper_bound=10.0)
            .alias(Col.uplift_factor_on_base_model)
        )
    )

    return EpBlendingTargets(
        target_points=target_points,
        weights=weights,
        blended=blended,
    )


def _add_rank_columns(ylt: pl.LazyFrame) -> pl.LazyFrame:
    return ylt.with_columns(
        pl.col(Col.loss)
        .rank(method="ordinal", descending=True)
        .over(Col.vendor, Col.modelled_lob, Col.rollup_peril)
        .cast(pl.Int64)
        .alias(Col.rnk)
    ).with_columns(
        pl.when(pl.col(Col.vendor) == "risklink")
        .then(100_000.0 / pl.col(Col.rnk))
        .otherwise(10_000.0 / pl.col(Col.rnk))
        .alias(Col.rp)
    ).with_columns(
        pl.when(pl.col(Col.rp) < 200)
        .then(pl.lit(0))
        .when(pl.col(Col.rp) < 1000)
        .then(pl.lit(200))
        .otherwise(pl.lit(1000))
        .alias(Col.rp_bucket)
    )


def apply_ep_blending_to_ylt(
    ylt: pl.LazyFrame,
    ep_blending_targets: EpBlendingTargets,
) -> pl.LazyFrame:
    factors = ep_blending_targets.blended.select(
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        pl.col(Col.return_period).alias(Col.rp_bucket),
        Col.ep_type,
        Col.risklink_loss,
        Col.verisk_loss,
        Col.target_loss,
        Col.base_model,
        Col.base_model_loss,
        Col.uplift_factor_on_base_model,
    )

    ylt_cols = ylt.collect_schema().names()
    return (
        ylt.join(
            factors,
            on=[
                Col.rollup_lob,
                Col.rollup_peril,
                Col.region_peril_id,
                Col.rp_bucket,
                Col.base_model,
            ],
            how="inner",
        )
        .with_columns(
            (pl.col(Col.loss) * pl.col(Col.uplift_factor_on_base_model)).alias(Col.loss),
            pl.lit("blended").alias(Col.metric),
        )
        .select(ylt_cols)
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
        Col.office,
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
        pl.col(RawCol.occ_year).alias(Col.year_id),
        pl.col(RawCol.factor).alias("_euws_factor_raw_source"),
    )

    gbp_forecast_cols = gbp_forecast.collect_schema().names()
    joined = (
        gbp_forecast.join(
            verisk_events,
            on=[Col.event_id, Col.year_id, Col.model_code],
            how="left",
        )
        .join(euws_factors, on=[Col.model_event_id, Col.year_id], how="left")
    )
    _log_defaulted_rows(
        joined,
        (pl.col(Col.rollup_peril) == "Europe_WS") & pl.col("_euws_factor_raw_source").is_null(),
        "euws factor defaulted rows=%d",
    )
    return (
        joined
        .with_columns(
            pl.when(pl.col(Col.rollup_peril) == "Europe_WS")
            .then(pl.col("_euws_factor_raw_source").fill_null(1.0))
            .otherwise(pl.lit(1.0))
            .alias("_euws_factor_raw")
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
        Col.office,
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
    return ylt.join(
        risklink_events,
        on=[Col.event_id, Col.region_peril_id],
        how="left",
    )


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


def build_event_validation_report(
    *fanouts: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame | pl.LazyFrame:
    reports = []
    for fanout in fanouts:
        reports.append(
            fanout.group_by(Col.base_model, Col.metric, Col.forecast_date).agg(
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


def _mts_output_dimensions(frame: pl.DataFrame | pl.LazyFrame) -> list[str]:
    return [
        col for col in frame.columns
        if col not in (Col.metric, Col.forecast_date, Col.loss)
        and not col.startswith("_")
    ]


def _write_combined_outputs(
    output_root: Path,
    ylt: pl.DataFrame,
    ylt_dialsup: pl.DataFrame,
) -> None:
    write_parquet_with_log(ylt, output_root / "mts_tbl_ylt_combined_all_factors.parquet")
    write_parquet_with_log(ylt_dialsup, output_root / "mts_tbl_ylt_dialsup.parquet")

    common_cols = [c for c in ylt.columns if c in ylt_dialsup.columns]
    dims = _mts_output_dimensions(ylt.select(common_cols))
    sel = [*dims, Col.metric, Col.forecast_date, Col.loss]
    final_metrics = ["euws_override", "dialsup_gbp_forecast"]

    combined = pl.concat(
        [ylt.filter(pl.col(Col.metric) == "euws_override").select(sel),
         ylt_dialsup.filter(pl.col(Col.metric) == "dialsup_gbp_forecast").select(sel)],
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

    write_parquet_with_log(wide, output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet")


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
    if isinstance(frame, pl.LazyFrame):
        frame = frame.collect(no_optimization=True)
    frame.write_parquet(output_path)
    logger.info(
        "wrote output=%s rows=%d elapsed=%.2fs",
        output_path,
        frame.height,
        time.perf_counter() - started,
    )


def _log_checkpoint(name: str, frame: pl.DataFrame) -> None:
    logger.info("checkpoint=%s rows=%d", name, frame.height)


def _warn_row_drop(name: str, before: int, after: int) -> None:
    if after < before:
        logger.warning("%s join dropped rows before=%d after=%d dropped=%d", name, before, after, before - after)


def _log_defaulted_rows(frame: pl.LazyFrame, condition: pl.Expr, message: str) -> None:
    defaulted_rows = frame.select(condition.sum().alias("defaulted_rows")).collect().item()
    if defaulted_rows > 0:
        logger.warning(message, defaulted_rows)


def write_mart_outputs(output_root: Path, result: PipelineRunResult) -> None:
    output_dir = output_root / "marts"
    output_dir.mkdir(parents=True, exist_ok=True)

    fanouts: dict[str, pl.DataFrame] = {}
    for name, frame in result.marts.frames.items():
        if not name.endswith("fanout"):
            continue
        started = time.perf_counter()
        logger.info("collecting fanout=%s", name)
        if isinstance(frame, pl.LazyFrame):
            fanouts[name] = frame.collect(no_optimization=True)
        else:
            fanouts[name] = frame
        logger.info(
            "collected fanout=%s rows=%d elapsed=%.2fs",
            name,
            fanouts[name].height,
            time.perf_counter() - started,
        )

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
        partitions = (
            frame.select(Col.forecast_date, Col.base_model, Col.metric)
            .unique()
            .sort(Col.forecast_date, Col.base_model, Col.metric)
        )
        for row in partitions.iter_rows(named=True):
            tag = forecast_tag(row[Col.forecast_date])
            vendor = hisco_vendor_label(row[Col.base_model])
            metric = row[Col.metric]
            output_path = output_dir / f"Hisco{vendor}_{tag}_{metric}.parquet"
            write_parquet_with_log(
                frame.filter(
                    (pl.col(Col.forecast_date) == row[Col.forecast_date])
                    & (pl.col(Col.base_model) == row[Col.base_model])
                    & (pl.col(Col.metric) == metric)
                ).select(
                    FanoutCol.ModelEventID,
                    FanoutCol.ModelYear,
                    FanoutCol.CurrencyCode,
                    FanoutCol.ModelYOA,
                    FanoutCol.ModelGrossLoss,
                    FanoutCol.ModelInwardsReinstatement,
                    FanoutCol.ModelEventDay,
                    FanoutCol.LossClassName,
                ),
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
    validation_inputs: PipelineValidationInputs | None = None,
) -> PipelineRunResult:
    data_root = Path(data_root)
    output_root = Path(output_root)
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

    with logged_phase("staging"):
        for filename, frame in seeds.frames.items():
            seed_frames[Path(filename).stem] = frame
        verisk_events = load_verisk_events(data_root)
        seed_frames["verisk_events"] = verisk_events
        risklink_events = load_risklink_flood_events(data_root)
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

        ep_blending_targets = calculate_ep_blending_targets(joined_ep_summaries, seeds)
        intermediate_frames["ep_blending_target_points"] = ep_blending_targets.target_points
        intermediate_frames["ep_blending_weights"] = ep_blending_targets.weights
        intermediate_frames["ep_blending_targets"] = ep_blending_targets.blended

        base_model_expr = (
            pl.when(pl.col(Col.rollup_peril).is_in(["Europe_FL", "UK_FL"]))
            .then(pl.lit("risklink"))
            .otherwise(pl.lit("verisk"))
        )
        ylt_original = enriched_ylts.combined.with_columns(
            base_model_expr.alias(Col.base_model),
            pl.lit("original").alias(Col.metric),
        ).filter(pl.col(Col.vendor) == pl.col(Col.base_model))
        intermediate_frames["ylt_original"] = ylt_original

        ylt_ranked = _add_rank_columns(ylt_original)
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
            base_model_expr.alias(Col.base_model),
            pl.lit("original").alias(Col.metric),
        ).filter(pl.col(Col.vendor) == pl.col(Col.base_model))
        intermediate_frames["ylt_original_dialsup"] = ylt_original_dialsup
        ylt_ranked_dialsup = _add_rank_columns(ylt_original_dialsup)
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

    with logged_phase("marts"):
        main_fanout = build_main_fanout(
            ylt.lazy().filter(pl.col(Col.metric) == "euws_override"),
            risklink_events,
        )
        mart_frames["main_fanout"] = main_fanout

        dialsup_fanout = build_dialsup_fanout(
            ylt_dialsup.lazy().filter(pl.col(Col.metric) == "dialsup_gbp_forecast"),
            risklink_events,
        )
        mart_frames["dialsup_fanout"] = dialsup_fanout

        mart_frames["event_validation"] = build_event_validation_report(
            main_fanout,
            dialsup_fanout,
        )
        mart_frames["ylt_long"] = ylt
        mart_frames["ylt_dialsup"] = ylt_dialsup

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
