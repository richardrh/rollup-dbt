from __future__ import annotations

import logging
import re
import time
import tempfile
from pathlib import Path
from typing import Any

import polars as pl


logger = logging.getLogger(__name__)

CANONICAL_COLUMNS = [
    "vendor",
    "analysis_id",
    "modelled_lob",
    "modelled_peril",
    "ep_type",
    "return_period",
    "loss",
]
REQUIRED_SOURCE_COLUMNS = ["id", "modelled_lob", "modelled_peril"]
METRIC_COLUMN_PATTERN = re.compile(r"^(AAL|AEP|OEP)_(0|[1-9]\d*)$")
RECOGNIZABLE_METRIC_COLUMN_PATTERN = re.compile(r"^(aal|aep|oep)_", re.IGNORECASE)


EP_SUMMARY_OUTPUT_FILENAMES = {
    "verisk": "verisk_ep_summary.long.csv",
    "risklink": "rms_ep_summary.long.csv",
}


def _check_vendor(vendor: str) -> None:
    if vendor not in EP_SUMMARY_OUTPUT_FILENAMES:
        known_vendors = ", ".join(EP_SUMMARY_OUTPUT_FILENAMES)
        raise ValueError(
            f"unknown EP summary vendor {vendor!r}; expected one of: {known_vendors}"
        )


def scan_ep_summary_csvs(data_root: Path | str, vendor: str) -> list[Path]:
    _check_vendor(vendor)
    source_dir = Path(data_root) / "ep_summaries" / vendor
    return sorted(
        (
            path
            for path in source_dir.glob("*.csv")
            if not path.name.endswith(".long.csv")
        ),
        key=lambda path: path.name.lower(),
    )


def build_ep_summary_from_wide_csv(csv_path: Path | str, vendor: str) -> pl.DataFrame:
    _check_vendor(vendor)
    csv_path = Path(csv_path)
    frame = pl.read_csv(csv_path, infer_schema=False)
    _validate_required_columns(frame, csv_path)

    invalid_metric_columns = [
        column
        for column in frame.columns
        if RECOGNIZABLE_METRIC_COLUMN_PATTERN.match(column)
        and not METRIC_COLUMN_PATTERN.fullmatch(column)
    ]
    if invalid_metric_columns:
        raise ValueError(
            f"{csv_path} contains noncanonical EP metric columns: {invalid_metric_columns}"
        )
    metric_columns = [
        column for column in frame.columns if METRIC_COLUMN_PATTERN.fullmatch(column)
    ]
    if not metric_columns:
        raise ValueError(
            f"{csv_path} does not contain metric columns like AAL_0, AEP_50, or OEP_100"
        )

    if "CatalogTypeCode" in frame.columns:
        frame = frame.filter(pl.col("CatalogTypeCode").str.strip_chars() == "STC")

    long_frame = frame.select([*REQUIRED_SOURCE_COLUMNS, *metric_columns]).unpivot(
        index=REQUIRED_SOURCE_COLUMNS,
        on=metric_columns,
        variable_name="metric",
        value_name="loss",
    )
    metric_metadata = pl.DataFrame(
        [_parse_metric_column(column) for column in metric_columns],
        schema={
            "metric": pl.String,
            "ep_type": pl.String,
            "return_period": pl.Int64,
        },
    )

    return (
        long_frame.join(metric_metadata, on="metric", how="left")
        .with_columns(
            pl.lit(vendor).alias("vendor"),
            pl.col("id").cast(pl.String).str.strip_chars().alias("analysis_id"),
            pl.col("modelled_lob").cast(pl.String).str.strip_chars(),
            pl.col("modelled_peril").cast(pl.String).str.strip_chars(),
            pl.col("loss")
            .cast(pl.String)
            .str.replace_all(r"[,\s]", "")
            .cast(pl.Float64, strict=False),
        )
        .filter(
            pl.col("analysis_id").is_not_null()
            & (pl.col("analysis_id") != "")
            & pl.col("modelled_lob").is_not_null()
            & (pl.col("modelled_lob") != "")
            & pl.col("modelled_peril").is_not_null()
            & (pl.col("modelled_peril") != "")
            & pl.col("loss").is_not_null()
        )
        .select(CANONICAL_COLUMNS)
    )


def generate_vendor_ep_summary(
    data_root: Path | str,
    vendor: str,
    csv_path: Path | str,
) -> Path:
    frame = build_ep_summary_from_wide_csv(csv_path, vendor)
    output_path = (
        Path(data_root) / "ep_summaries" / vendor / EP_SUMMARY_OUTPUT_FILENAMES[vendor]
    )
    return _write_ep_summary(frame, output_path)


def generate_ep_summaries(data_root: Path | str = "data") -> list[Path]:
    data_root = Path(data_root)
    output_paths: list[Path] = []
    for vendor in EP_SUMMARY_OUTPUT_FILENAMES:
        source_files = scan_ep_summary_csvs(data_root, vendor)
        if not source_files:
            source_dir = data_root / "ep_summaries" / vendor
            raise FileNotFoundError(f"No source CSV files found in {source_dir}.")
        if len(source_files) > 1:
            raise ValueError(
                f"Multiple source CSV files found for {vendor}: {source_files}; "
                "select explicitly with generate_vendor_ep_summary or CLI --vendor/--csv."
            )
        output_paths.append(
            generate_vendor_ep_summary(data_root, vendor, source_files[0])
        )
    return output_paths


def _write_ep_summary(frame: pl.DataFrame, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    logger.info(
        "writing output=%s",
        output_path,
        extra={"event": "ep_summary_write_start", "path": output_path},
    )
    with tempfile.NamedTemporaryFile(
        suffix=".csv",
        prefix=f".{output_path.stem}-",
        dir=output_path.parent,
        delete=False,
    ) as handle:
        staged_path = Path(handle.name)
    try:
        frame.write_csv(staged_path)
        staged_path.replace(output_path)
    finally:
        staged_path.unlink(missing_ok=True)
    elapsed_seconds = time.perf_counter() - started
    logger.info(
        "wrote output=%s rows=%d elapsed=%.2fs",
        output_path,
        frame.height,
        elapsed_seconds,
        extra={
            "event": "ep_summary_write",
            "path": output_path,
            "rows": frame.height,
            "elapsed_seconds": elapsed_seconds,
        },
    )
    return output_path


def _validate_required_columns(frame: pl.DataFrame, csv_path: Path) -> None:
    missing_columns = [
        column for column in REQUIRED_SOURCE_COLUMNS if column not in frame.columns
    ]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(
            f"{csv_path} is missing required EP summary columns: {missing}"
        )


def _parse_metric_column(metric_name: str) -> dict[str, Any]:
    match = METRIC_COLUMN_PATTERN.fullmatch(metric_name)
    if match is None:
        raise ValueError(f"unsupported EP metric column: {metric_name}")

    ep_type = match.group(1)
    return_period = int(match.group(2))
    if ep_type == "AAL" and return_period != 0:
        raise ValueError(
            f"unsupported EP metric column: {metric_name}; AAL must be AAL_0"
        )
    return {"metric": metric_name, "ep_type": ep_type, "return_period": return_period}
