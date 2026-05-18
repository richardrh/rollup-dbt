from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandera.polars as pa
import polars as pl
import yaml


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
                row_count = file_frame.select(pl.len().alias("row_count")).collect().item()

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
        "verisk": (data_root / "ylt" / "verisk", "GroundUpLoss", 10_000),
        "risklink": (data_root / "ylt" / "risklink", "loss", 100_000),
    }
    rows: list[dict[str, object]] = []

    for vendor, (folder, loss_column, divisor) in vendor_specs.items():
        for file_path in sorted(folder.glob("*.parquet")):
            frame = pl.scan_parquet(file_path)
            loss_sum = frame.select(pl.col(loss_column).sum().alias("loss_sum")).collect().item()
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


def load_verisk_events(data_root: Path | str = "data") -> pl.LazyFrame:
    return pl.scan_parquet(
        Path(data_root) / "seeds" / "validation" / "verisk_events.parquet"
    ).select(
        pl.col("EventID").alias("model_event_id"),
        pl.col("ModelID").alias("model_code"),
        pl.col("Event").alias("event_id"),
        pl.col("Year").alias("year_id"),
        pl.col("Day").alias("event_day"),
    )


def load_risklink_flood_events(data_root: Path | str = "data") -> pl.LazyFrame:
    return (
        pl.scan_parquet(
            Path(data_root) / "seeds" / "validation" / "risklink_flood22_model_events.parquet"
        )
        .group_by("ModelEventID", "RegionPerilID")
        .agg(pl.col("ModelOccurrenceDate").min().alias("model_occurrence_date"))
        .select(
            pl.col("ModelEventID").alias("event_id"),
            pl.col("RegionPerilID").alias("region_peril_id"),
            pl.col("model_occurrence_date")
            .dt.ordinal_day()
            .cast(pl.Int64)
            .alias("risklink_event_day"),
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
            row_count = file_frame.select(pl.len().alias("row_count")).collect().item()

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
    verisk = ylt.frames.verisk.filter(pl.col("CatalogTypeCode") == "STC").select(
        pl.lit("verisk").alias("vendor"),
        pl.col("Analysis").cast(pl.String).alias("analysis_id"),
        pl.col("Analysis").cast(pl.String).alias("modelled_peril"),
        pl.col("ExposureAttribute").cast(pl.String).alias("modelled_lob"),
        pl.col("ModelCode").cast(pl.Int64).alias("model_code"),
        pl.col("YearID").cast(pl.Int64).alias("year_id"),
        pl.col("EventID").cast(pl.Int64).alias("event_id"),
        pl.col("GroundUpLoss").cast(pl.Float64).alias("loss"),
    )

    risklink = ylt.frames.risklink.select(
        pl.lit("risklink").alias("vendor"),
        pl.col("anlsid").cast(pl.String).alias("analysis_id"),
        pl.lit(None).cast(pl.String).alias("modelled_peril"),
        pl.lit(None).cast(pl.String).alias("modelled_lob"),
        pl.lit(None).cast(pl.Int64).alias("model_code"),
        pl.col("yearid").cast(pl.Int64).alias("year_id"),
        pl.col("eventid").cast(pl.Int64).alias("event_id"),
        pl.col("loss").cast(pl.Float64).alias("loss"),
    )

    return NormalizedYltFrames(verisk=verisk, risklink=risklink)


def enrich_ylt_with_ep_summaries(
    normalized_ylt: NormalizedYltFrames,
    staged_ep_summaries: StagedEpSummaries,
) -> EnrichedYltFrames:
    ep_summary = staged_ep_summaries.selected

    verisk_keys = (
        ep_summary.filter(pl.col("vendor") == "verisk")
        .select(
            "vendor",
            "modelled_lob",
            "modelled_peril",
            "rollup_lob",
            "rollup_peril",
            "region_peril_id",
            "cds_cat_class_name",
            "class",
            "office",
            "currency",
        )
        .unique()
    )
    verisk = normalized_ylt.verisk.join(
        verisk_keys,
        on=["vendor", "modelled_lob", "modelled_peril"],
        how="inner",
    ).select(
        "vendor",
        "analysis_id",
        "modelled_lob",
        "modelled_peril",
        "rollup_lob",
        "rollup_peril",
        "region_peril_id",
        "cds_cat_class_name",
        "class",
        "office",
        "currency",
        "model_code",
        "year_id",
        "event_id",
        "loss",
    )

    risklink_lookup = (
        ep_summary.filter(pl.col("vendor") == "risklink")
        .select(
            "vendor",
            "analysis_id",
            "modelled_lob",
            "modelled_peril",
            "rollup_lob",
            "rollup_peril",
            "region_peril_id",
            "cds_cat_class_name",
            "class",
            "office",
            "currency",
        )
        .unique()
    )
    risklink = (
        normalized_ylt.risklink.drop("modelled_lob", "modelled_peril")
        .join(risklink_lookup, on=["vendor", "analysis_id"], how="inner")
        .select(
            "vendor",
            "analysis_id",
            "modelled_lob",
            "modelled_peril",
            "rollup_lob",
            "rollup_peril",
            "region_peril_id",
            "cds_cat_class_name",
            "class",
            "office",
            "currency",
            "model_code",
            "year_id",
            "event_id",
            "loss",
        )
    )

    combined = pl.concat([verisk, risklink], how="vertical")

    return EnrichedYltFrames(verisk=verisk, risklink=risklink, combined=combined)


def stage_ep_summaries(
    ep_summaries: EpSummaryValidationResult,
    seeds: SeedValidationResult,
) -> StagedEpSummaries:
    lobs = seeds.frames["lobs.csv"].lazy().select(
        "modelled_lob",
        "rollup_lob",
        "cds_cat_class_name",
        "class",
        "office",
        "currency",
    )
    perils = seeds.frames["perils.csv"].lazy().select(
        "modelled_peril",
        "rollup_peril",
        "region",
        "peril",
        "region_peril_id",
    )

    enriched = (
        ep_summaries.frame.join(lobs, on="modelled_lob", how="left")
        .join(perils, on="modelled_peril", how="left")
    )

    priority = (
        pl.when((pl.col("vendor") == "risklink") & (pl.col("rollup_peril") == "Europe_EQ") & (pl.col("modelled_peril") == "EUxESGB EQ Adj"))
        .then(1)
        .when((pl.col("vendor") == "risklink") & (pl.col("rollup_peril") == "Europe_EQ") & (pl.col("modelled_peril") == "EUxESGB EQ"))
        .then(2)
        .when((pl.col("vendor") == "risklink") & (pl.col("rollup_peril") == "Europe_WS") & (pl.col("modelled_peril") == "EUxGB WS CVV (FlrArea)"))
        .then(1)
        .when((pl.col("vendor") == "risklink") & (pl.col("rollup_peril") == "Europe_WS") & (pl.col("modelled_peril") == "EUxGB WS CVV"))
        .then(2)
        .when((pl.col("vendor") == "risklink") & (pl.col("rollup_peril") == "Europe_WS") & (pl.col("modelled_peril") == "EUxGB WS (FlrArea)"))
        .then(3)
        .when((pl.col("vendor") == "risklink") & (pl.col("rollup_peril") == "Europe_WS") & (pl.col("modelled_peril") == "EUxGB WS"))
        .then(4)
        .when((pl.col("vendor") == "risklink") & (pl.col("rollup_peril") == "UK_WS") & (pl.col("modelled_peril") == "GB WSSS CVV (FlrArea)"))
        .then(1)
        .when((pl.col("vendor") == "risklink") & (pl.col("rollup_peril") == "UK_WS") & (pl.col("modelled_peril") == "GB WSSS CVV"))
        .then(2)
        .when((pl.col("vendor") == "risklink") & (pl.col("rollup_peril") == "UK_WS") & (pl.col("modelled_peril") == "GB WSSS (FlrArea)"))
        .then(3)
        .when((pl.col("vendor") == "risklink") & (pl.col("rollup_peril") == "UK_WS") & (pl.col("modelled_peril") == "GB WSSS"))
        .then(4)
        .when((pl.col("vendor") == "verisk") & (pl.col("rollup_peril") == "Europe_EQ") & (pl.col("modelled_peril") == "EU_EQ"))
        .then(1)
        .when((pl.col("vendor") == "verisk") & (pl.col("rollup_peril") == "Europe_FL") & (pl.col("modelled_peril") == "EU_FL"))
        .then(1)
        .when((pl.col("vendor") == "verisk") & (pl.col("rollup_peril") == "Europe_WS") & (pl.col("modelled_peril") == "EU_WS_GCAdj"))
        .then(1)
        .when((pl.col("vendor") == "verisk") & (pl.col("rollup_peril") == "Europe_WS") & (pl.col("modelled_peril") == "EU_WS"))
        .then(2)
        .when((pl.col("vendor") == "verisk") & (pl.col("rollup_peril") == "UK_FL") & (pl.col("modelled_peril") == "UK_FL"))
        .then(1)
        .when((pl.col("vendor") == "verisk") & (pl.col("rollup_peril") == "UK_WS") & (pl.col("modelled_peril") == "UK_WSSS_GCAdj"))
        .then(1)
        .when((pl.col("vendor") == "verisk") & (pl.col("rollup_peril") == "UK_WS") & (pl.col("modelled_peril") == "UK_WSSS"))
        .then(2)
        .otherwise(100)
        .alias("selection_priority")
    )

    enriched = enriched.with_columns(priority)
    selection_keys = ["vendor", "rollup_lob", "rollup_peril"]
    selected_candidates = enriched.select(
        *selection_keys,
        "modelled_peril",
        "selection_priority",
    ).unique()
    selected_priorities = selected_candidates.group_by(selection_keys).agg(
        pl.col("selection_priority").min()
    )
    selected_modelled_perils = (
        selected_candidates.join(
            selected_priorities,
            on=[*selection_keys, "selection_priority"],
            how="inner",
        )
        .sort([*selection_keys, "selection_priority", "modelled_peril"])
        .group_by(selection_keys, maintain_order=True)
        .first()
        .select(*selection_keys, "modelled_peril")
    )
    selected = enriched.join(
        selected_modelled_perils,
        on=[*selection_keys, "modelled_peril"],
        how="inner",
    )

    return StagedEpSummaries(enriched=enriched, selected=selected)


def join_ep_summaries(
    staged_ep_summaries: StagedEpSummaries,
) -> JoinedEpSummaries:
    enriched = staged_ep_summaries.selected

    join_keys = ["rollup_lob", "rollup_peril", "region_peril_id", "ep_type", "return_period"]
    verisk = (
        enriched.filter(pl.col("vendor") == "verisk")
        .group_by(join_keys)
        .agg(pl.col("loss").sum().alias("verisk_loss"))
    )
    risklink = (
        enriched.filter(pl.col("vendor") == "risklink")
        .group_by(join_keys)
        .agg(pl.col("loss").sum().alias("risklink_loss"))
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
        ((pl.col("ep_type") == "AAL") & (pl.col("return_period") == 0))
        | ((pl.col("ep_type") == "OEP") & (pl.col("return_period").is_in([200, 1000])))
    )

    weights = (
        seeds.frames["blending_factors.csv"]
        .lazy()
        .filter(
            (pl.col("RegionPerilID") != 216)
            | (pl.col("SubRegionPerilID") == "216b")
        )
        .sort("SubRegionPerilID")
        .group_by("RegionPerilID")
        .first()
        .select(
            pl.col("RegionPerilID").alias("region_peril_id"),
            pl.col("SubRegionPerilID").alias("sub_region_peril_id"),
            pl.col("SubRegionPeril").alias("sub_region_peril"),
            pl.col("AIRBlend").cast(pl.Float64).alias("verisk_weight"),
            pl.col("RMSBlend").cast(pl.Float64).alias("risklink_weight"),
        )
    )

    blended = (
        target_points.filter(
            pl.col("risklink_loss").is_not_null()
            & pl.col("verisk_loss").is_not_null()
        )
        .join(weights, on="region_peril_id", how="left")
        .with_columns(
            (
                (pl.col("verisk_loss") * pl.col("verisk_weight"))
                + (pl.col("risklink_loss") * pl.col("risklink_weight"))
            ).alias("target_loss")
        )
        .with_columns(
            pl.when(pl.col("rollup_peril").is_in(["Europe_FL", "UK_FL"]))
            .then(pl.lit("risklink"))
            .otherwise(pl.lit("verisk"))
            .alias("base_model")
        )
        .with_columns(
            pl.when(pl.col("base_model") == "risklink")
            .then(pl.col("risklink_loss"))
            .otherwise(pl.col("verisk_loss"))
            .alias("base_model_loss")
        )
        .with_columns(
            (pl.col("target_loss") / pl.col("base_model_loss")).alias(
                "uplift_factor_on_base_model"
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
        pl.when(pl.col("rollup_peril").is_in(["Europe_FL", "UK_FL"]))
        .then(pl.lit("risklink"))
        .otherwise(pl.lit("verisk"))
    )
    base_model_only = (
        enriched_ylts.combined.with_columns(base_model_expr.alias("base_model"))
        .filter(pl.col("vendor") == pl.col("base_model"))
    )

    ranked = (
        base_model_only.with_columns(
            pl.col("loss")
            .rank(method="ordinal", descending=True)
            .over("modelled_lob", "rollup_peril")
            .cast(pl.Int64)
            .alias("rnk")
        )
        .with_columns(
            pl.when(pl.col("vendor") == "risklink")
            .then(100_000.0 / pl.col("rnk"))
            .otherwise(10_000.0 / pl.col("rnk"))
            .alias("rp")
        )
        .with_columns(
            pl.when(pl.col("rp") < 200)
            .then(pl.lit(0))
            .when(pl.col("rp") < 1000)
            .then(pl.lit(200))
            .otherwise(pl.lit(1000))
            .alias("rp_bucket"),
        )
    )

    factors = ep_blending_targets.blended.select(
        "rollup_lob",
        "rollup_peril",
        "region_peril_id",
        pl.col("return_period").alias("rp_bucket"),
        "ep_type",
        "risklink_loss",
        "verisk_loss",
        "target_loss",
        "base_model",
        "base_model_loss",
        "uplift_factor_on_base_model",
    )

    blended = (
        ranked.join(
            factors,
            on=[
                "rollup_lob",
                "rollup_peril",
                "region_peril_id",
                "rp_bucket",
                "base_model",
            ],
            how="inner",
        )
        .with_columns(
            pl.col("loss").alias("original_ylt_loss"),
            (pl.col("loss") * pl.col("uplift_factor_on_base_model")).alias(
                "original_ylt_loss_blended"
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
        .filter(pl.col("target_currency") == "GBP")
        .select(
            pl.col("currency_code").alias("currency"),
            "target_currency",
            pl.col("rate_date").alias("fx_rate_date"),
            pl.col("rate").alias("fx_rate"),
        )
    )

    return blended_ylt.join(fx_rates, on="currency", how="inner").with_columns(
        (
            pl.col("original_ylt_loss_blended") * pl.col("fx_rate")
        ).alias("original_ylt_loss_blended_gbp")
    )


def apply_forecast_to_ylt(
    fx_ylt: pl.LazyFrame,
    seeds: SeedValidationResult,
) -> pl.LazyFrame:
    forecast_factors = seeds.frames["forecast_factors.csv"].lazy()
    forecast_dates = forecast_factors.select("forecast_date").unique()
    forecast_factors = forecast_factors.select(
        "class",
        "office",
        "forecast_date",
        pl.col("factor").alias("forecast_factor_raw"),
    )

    return (
        fx_ylt.join(forecast_dates, how="cross")
        .join(forecast_factors, on=["class", "office", "forecast_date"], how="left")
        .with_columns(
            pl.col("forecast_factor_raw")
            .fill_null(1.0)
            .alias("forecast_factor"),
            (
                pl.col("original_ylt_loss_blended_gbp")
                * pl.col("forecast_factor_raw").fill_null(1.0)
            ).alias("original_ylt_loss_blended_gbp_forecast")
        )
        .drop("forecast_factor_raw")
    )


def apply_euws_to_ylt(
    forecast_ylt: pl.LazyFrame,
    verisk_events: pl.LazyFrame,
    seeds: SeedValidationResult,
) -> pl.LazyFrame:
    euws_factors = seeds.frames["euws_rate_factors.csv"].lazy().select(
        "model_event_id",
        pl.col("occ_year").alias("year_id"),
        pl.col("factor").alias("euws_factor_raw_source"),
    )

    return (
        forecast_ylt.join(
            verisk_events,
            on=["event_id", "year_id", "model_code"],
            how="left",
        )
        .join(euws_factors, on=["model_event_id", "year_id"], how="left")
        .with_columns(
            pl.when(pl.col("rollup_peril") == "Europe_WS")
            .then(pl.col("euws_factor_raw_source").fill_null(1.0))
            .otherwise(pl.lit(1.0))
            .alias("euws_factor_raw")
        )
        .with_columns(
            (
                pl.col("original_ylt_loss_blended_gbp_forecast")
                * pl.col("euws_factor_raw")
            ).alias("original_ylt_loss_blended_gbp_forecast_euws_raw")
        )
        .drop("euws_factor_raw_source")
    )


def apply_euws_overrides_to_ylt(
    euws_ylt: pl.LazyFrame,
    seeds: SeedValidationResult,
) -> pl.LazyFrame:
    overrides = seeds.frames["euws_rank_overrides.csv"].lazy().select(
        "rollup_lob",
        pl.col("max_rank").alias("euws_override_max_rank"),
        pl.col("factor").alias("euws_override_factor"),
    )
    override_condition = (
        pl.col("euws_override_factor").is_not_null()
        & (pl.col("rnk") <= pl.col("euws_override_max_rank"))
        & (pl.col("euws_factor_raw") == 0)
    )

    return (
        euws_ylt.join(overrides, on="rollup_lob", how="left")
        .with_columns(
            override_condition.alias("euws_override_applied"),
            pl.when(override_condition)
            .then(pl.col("euws_override_factor"))
            .otherwise(pl.col("euws_factor_raw"))
            .alias("euws_factor"),
        )
        .with_columns(
            (
                pl.col("original_ylt_loss_blended_gbp_forecast")
                * pl.col("euws_factor")
            ).alias("original_ylt_loss_blended_gbp_forecast_euws")
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
        .filter(pl.col("target_currency") == "GBP")
        .select(
            pl.col("currency_code").alias("currency"),
            "target_currency",
            pl.col("rate_date").alias("fx_rate_date"),
            pl.col("rate").alias("fx_rate"),
        )
    )
    forecast_factors = seeds.frames["forecast_factors.csv"].lazy()
    forecast_dates = forecast_factors.select("forecast_date").unique()
    forecast_factors = forecast_factors.select(
        "class",
        "office",
        "forecast_date",
        pl.col("factor").alias("forecast_factor_raw"),
    )

    return (
        base_model_ylt.join(
            verisk_events,
            on=["event_id", "year_id", "model_code"],
            how="left",
        )
        .join(fx_rates, on="currency", how="inner")
        .join(forecast_dates, how="cross")
        .join(forecast_factors, on=["class", "office", "forecast_date"], how="left")
        .with_columns(
            pl.col("loss").alias("dialsup_original_ylt_loss"),
            pl.col("forecast_factor_raw")
            .fill_null(1.0)
            .alias("forecast_factor"),
        )
        .with_columns(
            (pl.col("dialsup_original_ylt_loss") * pl.col("fx_rate")).alias(
                "dialsup_loss_gbp"
            )
        )
        .with_columns(
            (pl.col("dialsup_loss_gbp") * pl.col("forecast_factor")).alias(
                "dialsup_loss_gbp_forecast"
            )
        )
        .drop("forecast_factor_raw")
    )


def enrich_risklink_event_days(
    ylt: pl.LazyFrame,
    risklink_events: pl.LazyFrame,
) -> pl.LazyFrame:
    return ylt.join(
        risklink_events,
        on=["event_id", "region_peril_id"],
        how="left",
    )


def build_main_fanout(
    ylt: pl.LazyFrame,
    risklink_events: pl.LazyFrame,
) -> pl.LazyFrame:
    ylt = enrich_risklink_event_days(ylt, risklink_events)
    return ylt.select(
        "forecast_date",
        "base_model",
        pl.lit("main").alias("metric"),
        pl.when(pl.col("base_model") == "risklink")
        .then(pl.col("event_id"))
        .otherwise(pl.col("model_event_id"))
        .cast(pl.Int64)
        .alias("ModelEventID"),
        pl.col("year_id").cast(pl.Int64).alias("ModelYear"),
        pl.col("target_currency").alias("CurrencyCode"),
        pl.lit(0).cast(pl.Int64).alias("ModelYOA"),
        pl.col("original_ylt_loss_blended_gbp_forecast_euws")
        .cast(pl.Float64)
        .alias("ModelGrossLoss"),
        pl.lit(0).cast(pl.Int64).alias("ModelInwardsReinstatement"),
        pl.when(pl.col("base_model") == "risklink")
        .then(pl.col("risklink_event_day"))
        .otherwise(pl.col("event_day"))
        .cast(pl.Int64)
        .alias("ModelEventDay"),
        pl.col("cds_cat_class_name").alias("LossClassName"),
    )


def build_dialsup_fanout(
    ylt: pl.LazyFrame,
    risklink_events: pl.LazyFrame,
) -> pl.LazyFrame:
    ylt = enrich_risklink_event_days(ylt, risklink_events)
    return ylt.select(
        "forecast_date",
        "base_model",
        pl.lit("dialsup").alias("metric"),
        pl.when(pl.col("base_model") == "risklink")
        .then(pl.col("event_id"))
        .otherwise(pl.col("model_event_id"))
        .cast(pl.Int64)
        .alias("ModelEventID"),
        pl.col("year_id").cast(pl.Int64).alias("ModelYear"),
        pl.col("target_currency").alias("CurrencyCode"),
        pl.lit(0).cast(pl.Int64).alias("ModelYOA"),
        pl.col("dialsup_loss_gbp_forecast")
        .cast(pl.Float64)
        .alias("ModelGrossLoss"),
        pl.lit(0).cast(pl.Int64).alias("ModelInwardsReinstatement"),
        pl.when(pl.col("base_model") == "risklink")
        .then(pl.col("risklink_event_day"))
        .otherwise(pl.col("event_day"))
        .cast(pl.Int64)
        .alias("ModelEventDay"),
        pl.col("cds_cat_class_name").alias("LossClassName"),
    )


def build_event_validation_report(*fanouts: pl.LazyFrame) -> pl.LazyFrame:
    reports = []
    for fanout in fanouts:
        reports.append(
            fanout.group_by("base_model", "metric", "forecast_date").agg(
                pl.len().alias("row_count"),
                pl.col("ModelEventID").is_null().sum().alias("missing_model_event_id"),
                pl.col("ModelEventDay").is_null().sum().alias("missing_model_event_day"),
            )
        )
    return pl.concat(reports, how="vertical")


def build_ylt_combined_all_factors(ylt: pl.LazyFrame) -> pl.LazyFrame:
    return ylt.select(
        "rp",
        "rp_bucket",
        "rnk",
        "vendor",
        "region_peril_id",
        "rollup_peril",
        "rollup_lob",
        "cds_cat_class_name",
        "model_code",
        "year_id",
        "event_id",
        "loss",
        "base_model",
        "uplift_factor_on_base_model",
        "forecast_date",
        "forecast_factor",
        "target_currency",
        "fx_rate",
        "original_ylt_loss",
        "original_ylt_loss_blended",
        "original_ylt_loss_blended_gbp",
        "original_ylt_loss_blended_gbp_forecast",
        "model_event_id",
        "event_day",
        "euws_factor_raw",
        "euws_factor",
        "euws_override_applied",
        "original_ylt_loss_blended_gbp_forecast_euws",
    )


def build_ylt_dialsup_wide(ylt: pl.LazyFrame) -> pl.LazyFrame:
    return ylt.select(
        "vendor",
        "region_peril_id",
        "rollup_peril",
        "rollup_lob",
        "cds_cat_class_name",
        "model_code",
        "year_id",
        "event_id",
        "loss",
        "base_model",
        "forecast_date",
        "forecast_factor",
        "target_currency",
        "fx_rate",
        "model_event_id",
        "event_day",
        "dialsup_original_ylt_loss",
        "dialsup_loss_gbp",
        "dialsup_loss_gbp_forecast",
    )


def write_debug_frame(
    debug_dir: Path,
    name: str,
    frame: pl.DataFrame | pl.LazyFrame,
) -> None:
    debug_dir.mkdir(parents=True, exist_ok=True)
    output_path = debug_dir / f"{name}.parquet"
    if isinstance(frame, pl.LazyFrame):
        frame.collect().write_parquet(output_path)
        return
    frame.write_parquet(output_path)


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

    root_output_dir = data_root / "output"
    for filename, frame_name in {
        "mts_tbl_ylt_combined_all_factors.parquet": "ylt_combined_all_factors",
        "mts_tbl_ylt_dialsup.parquet": "ylt_dialsup_wide",
        "mts_event_validation.parquet": "event_validation",
    }.items():
        frame = result.marts.frames.get(frame_name)
        if frame is not None:
            frame.collect().write_parquet(root_output_dir / filename)

    for name, frame in result.marts.frames.items():
        if not name.endswith("fanout"):
            continue
        partitions = (
            frame.select("forecast_date", "base_model", "metric")
            .unique()
            .collect()
            .sort("forecast_date", "base_model", "metric")
        )
        for row in partitions.iter_rows(named=True):
            tag = forecast_tag(row["forecast_date"])
            vendor = hisco_vendor_label(row["base_model"])
            metric = row["metric"]
            output_path = output_dir / f"Hisco{vendor}_{tag}_{metric}.parquet"
            (
                frame.filter(
                    (pl.col("forecast_date") == row["forecast_date"])
                    & (pl.col("base_model") == row["base_model"])
                    & (pl.col("metric") == metric)
                )
                .select(
                    "ModelEventID",
                    "ModelYear",
                    "CurrencyCode",
                    "ModelYOA",
                    "ModelGrossLoss",
                    "ModelInwardsReinstatement",
                    "ModelEventDay",
                    "LossClassName",
                )
                .collect()
                .write_parquet(output_path)
            )


def run(data_root: Path | str = "data", *, debug: bool = False) -> PipelineRunResult:
    data_root = Path(data_root)
    seed_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {}
    staging_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {}
    intermediate_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {}
    mart_frames: dict[str, pl.DataFrame | pl.LazyFrame] = {}

    # STAGING
    seeds = load_validated_seed_frames(data_root)
    for filename, frame in seeds.frames.items():
        seed_frames[Path(filename).stem] = frame
    verisk_events = load_verisk_events(data_root)
    seed_frames["verisk_events"] = verisk_events
    risklink_events = load_risklink_flood_events(data_root)
    seed_frames["risklink_flood_events"] = risklink_events
    staging_frames["validation_seeds"] = seeds.report

    ylts = load_validated_ylt_frames(data_root)
    staging_frames["validation_ylt"] = ylts.report

    normalized_ylts = normalize_ylt(ylts)
    staging_frames["ylt_verisk_normalized"] = normalized_ylts.verisk
    staging_frames["ylt_risklink_normalized"] = normalized_ylts.risklink

    ep_summaries = load_validated_ep_summary_frames(data_root)
    staging_frames["validation_ep_summaries"] = ep_summaries.report
    staging_frames["ep_summaries"] = ep_summaries.frame

    staged_ep_summaries = stage_ep_summaries(ep_summaries, seeds)
    staging_frames["ep_summaries_enriched"] = staged_ep_summaries.enriched
    staging_frames["ep_summaries_selected"] = staged_ep_summaries.selected

    # TODO : We need to validate the 'validation' parquet files that are used at
    # the end of the process

    # INTERMEDIATE
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

    # Marts
    # TODO: We validate the output against the validation files

    result = PipelineRunResult(
        seeds=PipelineStage(seed_frames),
        staging=PipelineStage(staging_frames),
        intermediate=PipelineStage(intermediate_frames),
        marts=PipelineStage(mart_frames),
    )

    if debug:
        write_debug_outputs(data_root, result)

    write_mart_outputs(data_root, result)

    return result


if __name__ == "__main__":
    # Configure logger here.
    run()
