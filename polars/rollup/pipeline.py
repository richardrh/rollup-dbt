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
class PipelineRunResult:
    seeds: SeedValidationResult
    ylts: YltValidationResult
    normalized_ylts: NormalizedYltFrames
    ep_summaries: EpSummaryValidationResult
    staged_ep_summaries: StagedEpSummaries
    enriched_ylts: EnrichedYltFrames
    joined_ep_summaries: JoinedEpSummaries


def normalize_ylt(ylt: YltValidationResult) -> NormalizedYltFrames:
    verisk = ylt.frames.verisk.select(
        pl.lit("verisk").alias("vendor"),
        pl.col("Analysis").cast(pl.String).alias("analysis_id"),
        pl.col("Analysis").cast(pl.String).alias("modelled_peril"),
        pl.col("ExposureAttribute").cast(pl.String).alias("modelled_lob"),
        pl.col("YearID").cast(pl.Int64).alias("year_id"),
        pl.col("EventID").cast(pl.Int64).alias("event_id"),
        pl.col("GroundUpLoss").cast(pl.Float64).alias("loss"),
    )

    risklink = ylt.frames.risklink.select(
        pl.lit("risklink").alias("vendor"),
        pl.col("anlsid").cast(pl.String).alias("analysis_id"),
        pl.lit(None).cast(pl.String).alias("modelled_peril"),
        pl.lit(None).cast(pl.String).alias("modelled_lob"),
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
        .select("vendor", "modelled_lob", "modelled_peril")
        .unique()
    )
    verisk = normalized_ylt.verisk.join(
        verisk_keys,
        on=["vendor", "modelled_lob", "modelled_peril"],
        how="semi",
    ).select(
        "vendor",
        "analysis_id",
        "modelled_lob",
        "modelled_peril",
        "year_id",
        "event_id",
        "loss",
    )

    risklink_lookup = (
        ep_summary.filter(pl.col("vendor") == "risklink")
        .select("vendor", "analysis_id", "modelled_lob", "modelled_peril")
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
    selected_modelled_perils = (
        enriched.select(*selection_keys, "modelled_peril", "selection_priority")
        .unique()
        .sort([*selection_keys, "selection_priority"])
        .group_by(selection_keys)
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

    join_keys = ["rollup_lob", "rollup_peril", "ep_type", "return_period"]
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

    for filename, frame in result.seeds.frames.items():
        write_debug_frame(debug_dir, f"seed_{Path(filename).stem}", frame)

    write_debug_frame(debug_dir, "validation_seeds", result.seeds.report)
    write_debug_frame(debug_dir, "validation_ylt", result.ylts.report)
    write_debug_frame(debug_dir, "validation_ep_summaries", result.ep_summaries.report)
    write_debug_frame(debug_dir, "normalized_ylt_verisk", result.normalized_ylts.verisk)
    write_debug_frame(debug_dir, "normalized_ylt_risklink", result.normalized_ylts.risklink)
    write_debug_frame(debug_dir, "stg_ep_summaries_enriched", result.staged_ep_summaries.enriched)
    write_debug_frame(debug_dir, "stg_ep_summaries_selected", result.staged_ep_summaries.selected)
    write_debug_frame(debug_dir, "enriched_ylt_verisk", result.enriched_ylts.verisk)
    write_debug_frame(debug_dir, "enriched_ylt_risklink", result.enriched_ylts.risklink)
    write_debug_frame(debug_dir, "enriched_ylt_combined", result.enriched_ylts.combined)
    write_debug_frame(debug_dir, "ep_summaries", result.ep_summaries.frame)
    write_debug_frame(debug_dir, "int_ep_summaries_enriched", result.joined_ep_summaries.enriched)
    write_debug_frame(debug_dir, "int_ep_summaries_verisk", result.joined_ep_summaries.verisk)
    write_debug_frame(debug_dir, "int_ep_summaries_risklink", result.joined_ep_summaries.risklink)
    write_debug_frame(debug_dir, "int_raw_ep", result.joined_ep_summaries.joined)


def run(data_root: Path | str = "data", *, debug: bool = False) -> PipelineRunResult:
    data_root = Path(data_root)

    # STAGING
    seeds = load_validated_seed_frames(data_root)
    ylts = load_validated_ylt_frames(data_root)
    normalized_ylts = normalize_ylt(ylts)
    ep_summaries = load_validated_ep_summary_frames(data_root)
    staged_ep_summaries = stage_ep_summaries(ep_summaries, seeds)

    # TODO : We need to validate the 'validation' parquet files that are used at
    # the end of the process

    # INTERMEDIATE
    enriched_ylts = enrich_ylt_with_ep_summaries(normalized_ylts, staged_ep_summaries)
    joined_ep_summaries = join_ep_summaries(staged_ep_summaries)



    # TODO: filter normalized ylts for only analysis ids or (modelled_lob, modelled_peril) combinations that are inside ep_summaries
    
    
    # TODO: Then join them side by side to get a joined_ep_summaries
    # we can call this int_raw_ep
    
    # TODO: Then join this to blending weights to get a int_ep_blending
    # TODO: Then apply forecast factors into int_ep_blending_forecast
    # TODO: Then fxrate int_ep_blending_forecast_fx
    # TODO: Then euws rate factor int_ep_blending_forecast_fx_euws
    # TODO: Then euws override int_ep_blending_forecast_fx_euws_override
    # TODO: then produce the dialsup alongside this.
    #

    # Marts
    # TODO: We validate the output against the validation files

    result = PipelineRunResult(
        seeds=seeds,
        ylts=ylts,
        normalized_ylts=normalized_ylts,
        ep_summaries=ep_summaries,
        staged_ep_summaries=staged_ep_summaries,
        enriched_ylts=enriched_ylts,
        joined_ep_summaries=joined_ep_summaries,
    )

    if debug:
        write_debug_outputs(data_root, result)

    return result


if __name__ == "__main__":
    run()
