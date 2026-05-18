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
) -> EnrichedYltFrames:
    ep_summary = staged_ep_summaries.selected

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
    )

    enriched = (
        ep_summaries.frame.join(lobs, on=Col.modelled_lob, how="left")
        .join(perils, on=Col.modelled_peril, how="left")
    )

    priority = (
        pl.when(
            (pl.col(Col.vendor) == "risklink")
            & (pl.col(Col.rollup_peril) == "Europe_EQ")
            & (pl.col(Col.modelled_peril) == "EUxESGB EQ Adj")
        )
        .then(1)
        .when(
            (pl.col(Col.vendor) == "risklink")
            & (pl.col(Col.rollup_peril) == "Europe_EQ")
            & (pl.col(Col.modelled_peril) == "EUxESGB EQ")
        )
        .then(2)
        .when(
            (pl.col(Col.vendor) == "risklink")
            & (pl.col(Col.rollup_peril) == "Europe_WS")
            & (pl.col(Col.modelled_peril) == "EUxGB WS CVV (FlrArea)")
        )
        .then(1)
        .when(
            (pl.col(Col.vendor) == "risklink")
            & (pl.col(Col.rollup_peril) == "Europe_WS")
            & (pl.col(Col.modelled_peril) == "EUxGB WS CVV")
        )
        .then(2)
        .when(
            (pl.col(Col.vendor) == "risklink")
            & (pl.col(Col.rollup_peril) == "Europe_WS")
            & (pl.col(Col.modelled_peril) == "EUxGB WS (FlrArea)")
        )
        .then(3)
        .when(
            (pl.col(Col.vendor) == "risklink")
            & (pl.col(Col.rollup_peril) == "Europe_WS")
            & (pl.col(Col.modelled_peril) == "EUxGB WS")
        )
        .then(4)
        .when(
            (pl.col(Col.vendor) == "risklink")
            & (pl.col(Col.rollup_peril) == "UK_WS")
            & (pl.col(Col.modelled_peril) == "GB WSSS CVV (FlrArea)")
        )
        .then(1)
        .when(
            (pl.col(Col.vendor) == "risklink")
            & (pl.col(Col.rollup_peril) == "UK_WS")
            & (pl.col(Col.modelled_peril) == "GB WSSS CVV")
        )
        .then(2)
        .when(
            (pl.col(Col.vendor) == "risklink")
            & (pl.col(Col.rollup_peril) == "UK_WS")
            & (pl.col(Col.modelled_peril) == "GB WSSS (FlrArea)")
        )
        .then(3)
        .when(
            (pl.col(Col.vendor) == "risklink")
            & (pl.col(Col.rollup_peril) == "UK_WS")
            & (pl.col(Col.modelled_peril) == "GB WSSS")
        )
        .then(4)
        .when(
            (pl.col(Col.vendor) == "verisk")
            & (pl.col(Col.rollup_peril) == "Europe_EQ")
            & (pl.col(Col.modelled_peril) == "EU_EQ")
        )
        .then(1)
        .when(
            (pl.col(Col.vendor) == "verisk")
            & (pl.col(Col.rollup_peril) == "Europe_FL")
            & (pl.col(Col.modelled_peril) == "EU_FL")
        )
        .then(1)
        .when(
            (pl.col(Col.vendor) == "verisk")
            & (pl.col(Col.rollup_peril) == "Europe_WS")
            & (pl.col(Col.modelled_peril) == "EU_WS_GCAdj")
        )
        .then(1)
        .when(
            (pl.col(Col.vendor) == "verisk")
            & (pl.col(Col.rollup_peril) == "Europe_WS")
            & (pl.col(Col.modelled_peril) == "EU_WS")
        )
        .then(2)
        .when(
            (pl.col(Col.vendor) == "verisk")
            & (pl.col(Col.rollup_peril) == "UK_FL")
            & (pl.col(Col.modelled_peril) == "UK_FL")
        )
        .then(1)
        .when(
            (pl.col(Col.vendor) == "verisk")
            & (pl.col(Col.rollup_peril) == "UK_WS")
            & (pl.col(Col.modelled_peril) == "UK_WSSS_GCAdj")
        )
        .then(1)
        .when(
            (pl.col(Col.vendor) == "verisk")
            & (pl.col(Col.rollup_peril) == "UK_WS")
            & (pl.col(Col.modelled_peril) == "UK_WSSS")
        )
        .then(2)
        .otherwise(100)
        .alias(Col.selection_priority)
    )

    enriched = enriched.with_columns(priority)
    selection_keys = [Col.vendor, Col.rollup_lob, Col.rollup_peril]
    selected_candidates = enriched.select(
        *selection_keys,
        Col.modelled_peril,
        Col.selection_priority,
    ).unique()
    selected_priorities = selected_candidates.group_by(selection_keys).agg(
        pl.col(Col.selection_priority).min()
    )
    selected_modelled_perils = (
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
    selected = enriched.join(
        selected_modelled_perils,
        on=[*selection_keys, Col.modelled_peril],
        how="inner",
    )

    return StagedEpSummaries(enriched=enriched, selected=selected)


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
    )

    return EpBlendingTargets(
        target_points=target_points,
        weights=weights,
        blended=blended,
    )


def apply_ep_blending_to_ylt(
    enriched_ylts: EnrichedYltFrames,
    ep_blending_targets: EpBlendingTargets,
) -> dict[str, pl.LazyFrame]:
    base_model_expr = (
        pl.when(pl.col(Col.rollup_peril).is_in(["Europe_FL", "UK_FL"]))
        .then(pl.lit("risklink"))
        .otherwise(pl.lit("verisk"))
    )
    base_model_only = (
        enriched_ylts.combined.with_columns(base_model_expr.alias(Col.base_model))
        .filter(pl.col(Col.vendor) == pl.col(Col.base_model))
    )

    ranked = (
        base_model_only.with_columns(
            pl.col(Col.loss)
            .rank(method="ordinal", descending=True)
            .over(Col.modelled_lob, Col.rollup_peril)
            .cast(pl.Int64)
            .alias(Col.rnk)
        )
        .with_columns(
            pl.when(pl.col(Col.vendor) == "risklink")
            .then(100_000.0 / pl.col(Col.rnk))
            .otherwise(10_000.0 / pl.col(Col.rnk))
            .alias(Col.rp)
        )
        .with_columns(
            pl.when(pl.col(Col.rp) < 200)
            .then(pl.lit(0))
            .when(pl.col(Col.rp) < 1000)
            .then(pl.lit(200))
            .otherwise(pl.lit(1000))
            .alias(Col.rp_bucket),
        )
    )

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

    blended = (
        ranked.join(
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
            pl.col(Col.loss).alias(Col.original_ylt_loss),
            (pl.col(Col.loss) * pl.col(Col.uplift_factor_on_base_model)).alias(
                Col.original_ylt_loss_blended
            ),
        )
    )

    return {
        "ylt_base_model": base_model_only,
        "ylt_ranked_bucketed": ranked,
        "ylt_blending_applied": blended,
    }


def apply_fx_to_ylt(
    blended_ylt: pl.LazyFrame,
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

    return blended_ylt.join(fx_rates, on=Col.currency, how="inner").with_columns(
        (
            pl.col(Col.original_ylt_loss_blended) * pl.col(Col.fx_rate)
        ).alias(Col.original_ylt_loss_blended_gbp)
    )


def apply_forecast_to_ylt(
    fx_ylt: pl.LazyFrame,
    seeds: SeedValidationResult,
) -> pl.LazyFrame:
    forecast_factors = seeds.frames["forecast_factors.csv"].lazy()
    forecast_dates = forecast_factors.select(Col.forecast_date).unique()
    forecast_factors = forecast_factors.select(
        Col.class_,
        Col.office,
        Col.forecast_date,
        pl.col(RawCol.factor).alias(Col.forecast_factor_raw),
    )

    return (
        fx_ylt.join(forecast_dates, how="cross")
        .join(
            forecast_factors,
            on=[Col.class_, Col.office, Col.forecast_date],
            how="left",
        )
        .with_columns(
            pl.col(Col.forecast_factor_raw)
            .fill_null(1.0)
            .alias(Col.forecast_factor),
            (
                pl.col(Col.original_ylt_loss_blended_gbp)
                * pl.col(Col.forecast_factor_raw).fill_null(1.0)
            ).alias(Col.original_ylt_loss_blended_gbp_forecast)
        )
        .drop(Col.forecast_factor_raw)
    )


def apply_euws_to_ylt(
    forecast_ylt: pl.LazyFrame,
    verisk_events: pl.LazyFrame,
    seeds: SeedValidationResult,
) -> pl.LazyFrame:
    euws_factors = seeds.frames["euws_rate_factors.csv"].lazy().select(
        Col.model_event_id,
        pl.col(RawCol.occ_year).alias(Col.year_id),
        pl.col(RawCol.factor).alias(Col.euws_factor_raw_source),
    )

    return (
        forecast_ylt.join(
            verisk_events,
            on=[Col.event_id, Col.year_id, Col.model_code],
            how="left",
        )
        .join(euws_factors, on=[Col.model_event_id, Col.year_id], how="left")
        .with_columns(
            pl.when(pl.col(Col.rollup_peril) == "Europe_WS")
            .then(pl.col(Col.euws_factor_raw_source).fill_null(1.0))
            .otherwise(pl.lit(1.0))
            .alias(Col.euws_factor_raw)
        )
        .with_columns(
            (
                pl.col(Col.original_ylt_loss_blended_gbp_forecast)
                * pl.col(Col.euws_factor_raw)
            ).alias(Col.original_ylt_loss_blended_gbp_forecast_euws_raw)
        )
        .drop(Col.euws_factor_raw_source)
    )


def apply_euws_overrides_to_ylt(
    euws_ylt: pl.LazyFrame,
    seeds: SeedValidationResult,
) -> pl.LazyFrame:
    overrides = seeds.frames["euws_rank_overrides.csv"].lazy().select(
        Col.rollup_lob,
        pl.col(RawCol.max_rank).alias(Col.euws_override_max_rank),
        pl.col(RawCol.factor).alias(Col.euws_override_factor),
    )
    override_condition = (
        pl.col(Col.euws_override_factor).is_not_null()
        & (pl.col(Col.rnk) <= pl.col(Col.euws_override_max_rank))
        & (pl.col(Col.euws_factor_raw) == 0)
    )

    return (
        euws_ylt.join(overrides, on=Col.rollup_lob, how="left")
        .with_columns(
            override_condition.alias(Col.euws_override_applied),
            pl.when(override_condition)
            .then(pl.col(Col.euws_override_factor))
            .otherwise(pl.col(Col.euws_factor_raw))
            .alias(Col.euws_factor),
        )
        .with_columns(
            (
                pl.col(Col.original_ylt_loss_blended_gbp_forecast)
                * pl.col(Col.euws_factor)
            ).alias(Col.original_ylt_loss_blended_gbp_forecast_euws)
        )
    )


def calculate_dialsup(
    base_model_ylt: pl.LazyFrame,
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
        pl.col(RawCol.factor).alias(Col.forecast_factor_raw),
    )

    return (
        base_model_ylt.join(
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
            pl.col(Col.loss).alias(Col.dialsup_original_ylt_loss),
            pl.col(Col.forecast_factor_raw)
            .fill_null(1.0)
            .alias(Col.forecast_factor),
        )
        .with_columns(
            (pl.col(Col.dialsup_original_ylt_loss) * pl.col(Col.fx_rate)).alias(
                Col.dialsup_loss_gbp
            )
        )
        .with_columns(
            (pl.col(Col.dialsup_loss_gbp) * pl.col(Col.forecast_factor)).alias(
                Col.dialsup_loss_gbp_forecast
            )
        )
        .drop(Col.forecast_factor_raw)
    )


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
        pl.lit("main").alias(Col.metric),
        pl.when(pl.col(Col.base_model) == "risklink")
        .then(pl.col(Col.event_id))
        .otherwise(pl.col(Col.model_event_id))
        .cast(pl.Int64)
        .alias(FanoutCol.ModelEventID),
        pl.col(Col.year_id).cast(pl.Int64).alias(FanoutCol.ModelYear),
        pl.col(Col.target_currency).alias(FanoutCol.CurrencyCode),
        pl.lit(0).cast(pl.Int64).alias(FanoutCol.ModelYOA),
        pl.col(Col.original_ylt_loss_blended_gbp_forecast_euws)
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
        pl.lit("dialsup").alias(Col.metric),
        pl.when(pl.col(Col.base_model) == "risklink")
        .then(pl.col(Col.event_id))
        .otherwise(pl.col(Col.model_event_id))
        .cast(pl.Int64)
        .alias(FanoutCol.ModelEventID),
        pl.col(Col.year_id).cast(pl.Int64).alias(FanoutCol.ModelYear),
        pl.col(Col.target_currency).alias(FanoutCol.CurrencyCode),
        pl.lit(0).cast(pl.Int64).alias(FanoutCol.ModelYOA),
        pl.col(Col.dialsup_loss_gbp_forecast)
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


def build_ylt_combined_all_factors(ylt: pl.LazyFrame) -> pl.LazyFrame:
    return ylt.select(
        Col.rp,
        Col.rp_bucket,
        Col.rnk,
        Col.vendor,
        Col.region_peril_id,
        Col.rollup_peril,
        Col.rollup_lob,
        Col.cds_cat_class_name,
        Col.model_code,
        Col.year_id,
        Col.event_id,
        Col.loss,
        Col.base_model,
        Col.uplift_factor_on_base_model,
        Col.forecast_date,
        Col.forecast_factor,
        Col.target_currency,
        Col.fx_rate,
        Col.original_ylt_loss,
        Col.original_ylt_loss_blended,
        Col.original_ylt_loss_blended_gbp,
        Col.original_ylt_loss_blended_gbp_forecast,
        Col.model_event_id,
        Col.event_day,
        Col.euws_factor_raw,
        Col.euws_factor,
        Col.euws_override_applied,
        Col.original_ylt_loss_blended_gbp_forecast_euws,
    )


def build_ylt_dialsup_wide(ylt: pl.LazyFrame) -> pl.LazyFrame:
    return ylt.select(
        Col.vendor,
        Col.region_peril_id,
        Col.rollup_peril,
        Col.rollup_lob,
        Col.cds_cat_class_name,
        Col.model_code,
        Col.year_id,
        Col.event_id,
        Col.loss,
        Col.base_model,
        Col.forecast_date,
        Col.forecast_factor,
        Col.target_currency,
        Col.fx_rate,
        Col.model_event_id,
        Col.event_day,
        Col.dialsup_original_ylt_loss,
        Col.dialsup_loss_gbp,
        Col.dialsup_loss_gbp_forecast,
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
    logger.info("writing output=%s", output_path)
    if isinstance(frame, pl.LazyFrame):
        frame = frame.collect()
    frame.write_parquet(output_path)
    logger.info(
        "wrote output=%s rows=%d elapsed=%.2fs",
        output_path,
        frame.height,
        time.perf_counter() - started,
    )


def write_debug_outputs(data_root: Path, result: PipelineRunResult) -> None:
    debug_dir = data_root / "output" / "debug"

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


def write_mart_outputs(data_root: Path, result: PipelineRunResult) -> None:
    output_dir = data_root / "output" / "marts"
    output_dir.mkdir(parents=True, exist_ok=True)

    fanouts: dict[str, pl.DataFrame] = {}
    for name, frame in result.marts.frames.items():
        if not name.endswith("fanout"):
            continue
        started = time.perf_counter()
        logger.info("collecting fanout=%s", name)
        fanouts[name] = frame.collect() if isinstance(frame, pl.LazyFrame) else frame
        logger.info(
            "collected fanout=%s rows=%d elapsed=%.2fs",
            name,
            fanouts[name].height,
            time.perf_counter() - started,
        )

    root_output_dir = data_root / "output"
    for filename, frame_name in {
        "mts_tbl_ylt_combined_all_factors.parquet": "ylt_combined_all_factors",
        "mts_tbl_ylt_dialsup.parquet": "ylt_dialsup_wide",
    }.items():
        frame = result.marts.frames.get(frame_name)
        if frame is not None:
            write_parquet_with_log(frame, root_output_dir / filename)

    if fanouts:
        write_parquet_with_log(
            build_event_validation_report(*fanouts.values()),
            root_output_dir / "mts_event_validation.parquet",
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


def run(data_root: Path | str = "data", *, debug: bool = False) -> PipelineRunResult:
    data_root = Path(data_root)
    seed_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {}
    staging_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {}
    intermediate_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {}
    mart_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {}

    with logged_phase("validation"):
        seeds = load_validated_seed_frames(data_root)
        ylts = load_validated_ylt_frames(data_root)
        ep_summaries = load_validated_ep_summary_frames(data_root)
        ensure_valid_inputs(seeds, ylts, ep_summaries)

    with logged_phase("staging"):
        for filename, frame in seeds.frames.items():
            seed_frames[Path(filename).stem] = frame
        verisk_events = load_verisk_events(data_root)
        seed_frames["verisk_events"] = verisk_events
        risklink_events = load_risklink_flood_events(data_root)
        seed_frames["risklink_flood_events"] = risklink_events
        staging_frames["validation_seeds"] = seeds.report

        staging_frames["validation_ylt"] = ylts.report

        normalized_ylts = normalize_ylt(ylts)
        staging_frames["ylt_verisk_normalized"] = normalized_ylts.verisk
        staging_frames["ylt_risklink_normalized"] = normalized_ylts.risklink

        staging_frames["validation_ep_summaries"] = ep_summaries.report
        staging_frames["ep_summaries"] = ep_summaries.frame

        staged_ep_summaries = stage_ep_summaries(ep_summaries, seeds)
        staging_frames["ep_summaries_enriched"] = staged_ep_summaries.enriched
        staging_frames["ep_summaries_selected"] = staged_ep_summaries.selected

    with logged_phase("intermediate"):
        enriched_ylts = enrich_ylt_with_ep_summaries(normalized_ylts, staged_ep_summaries)
        intermediate_frames["ylt_verisk_enriched"] = enriched_ylts.verisk
        intermediate_frames["ylt_risklink_enriched"] = enriched_ylts.risklink
        intermediate_frames["ylt_combined_enriched"] = enriched_ylts.combined

        joined_ep_summaries = join_ep_summaries(staged_ep_summaries)
        intermediate_frames["ep_summaries_enriched"] = joined_ep_summaries.enriched
        intermediate_frames["ep_summaries_verisk"] = joined_ep_summaries.verisk
        intermediate_frames["ep_summaries_risklink"] = joined_ep_summaries.risklink
        intermediate_frames["ep_vendor_joined"] = joined_ep_summaries.joined

        ep_blending_targets = calculate_ep_blending_targets(joined_ep_summaries, seeds)
        intermediate_frames["ep_blending_target_points"] = ep_blending_targets.target_points
        intermediate_frames["ep_blending_weights"] = ep_blending_targets.weights
        intermediate_frames["ep_blending_targets"] = ep_blending_targets.blended

        ylt_blending_frames = apply_ep_blending_to_ylt(enriched_ylts, ep_blending_targets)
        intermediate_frames.update(ylt_blending_frames)

        ylt_dialsup = calculate_dialsup(ylt_blending_frames["ylt_base_model"], verisk_events, seeds)
        intermediate_frames["ylt_dialsup"] = ylt_dialsup

        ylt_fx_applied = apply_fx_to_ylt(ylt_blending_frames["ylt_blending_applied"], seeds)
        intermediate_frames["ylt_fx_applied"] = ylt_fx_applied

        ylt_forecast_applied = apply_forecast_to_ylt(ylt_fx_applied, seeds)
        intermediate_frames["ylt_forecast_applied"] = ylt_forecast_applied

        ylt_euws_applied = apply_euws_to_ylt(ylt_forecast_applied, verisk_events, seeds)
        intermediate_frames["ylt_euws_applied"] = ylt_euws_applied

        ylt_euws_override_applied = apply_euws_overrides_to_ylt(ylt_euws_applied, seeds)
        intermediate_frames["ylt_euws_override_applied"] = ylt_euws_override_applied

    with logged_phase("marts"):
        main_fanout = build_main_fanout(ylt_euws_override_applied, risklink_events)
        mart_frames["main_fanout"] = main_fanout

        dialsup_fanout = build_dialsup_fanout(ylt_dialsup, risklink_events)
        mart_frames["dialsup_fanout"] = dialsup_fanout

        mart_frames["event_validation"] = build_event_validation_report(
            main_fanout,
            dialsup_fanout,
        )
        mart_frames["ylt_combined_all_factors"] = build_ylt_combined_all_factors(
            ylt_euws_override_applied,
        )
        mart_frames["ylt_dialsup_wide"] = build_ylt_dialsup_wide(ylt_dialsup)

    result = PipelineRunResult(
        seeds=PipelineStage(seed_frames),
        staging=PipelineStage(staging_frames),
        intermediate=PipelineStage(intermediate_frames),
        marts=PipelineStage(mart_frames),
    )

    if debug:
        with logged_phase("debug_outputs"):
            write_debug_outputs(data_root, result)

    with logged_phase("write_outputs"):
        write_mart_outputs(data_root, result)

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
