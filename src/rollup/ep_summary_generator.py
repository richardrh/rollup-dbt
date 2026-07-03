from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
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
SOURCE_ALIASES = {
    "ExposureAttribute": "modelled_lob",
    "Analysis": "modelled_peril",
}
METRIC_COLUMN_PATTERN = re.compile(r"^(aal|aep|oep)_(\d+(?:\.0)?)$", re.IGNORECASE)


@dataclass(frozen=True)
class EpSummaryVendorConfig:
    vendor: str
    source_dirname: str
    output_filename: str

    def source_dir(self, data_root: Path | str) -> Path:
        return Path(data_root) / "ep_summaries" / self.source_dirname

    def output_path(self, data_root: Path | str) -> Path:
        return self.source_dir(data_root) / self.output_filename


EP_SUMMARY_VENDOR_CONFIGS = {
    "verisk": EpSummaryVendorConfig(
        vendor="verisk",
        source_dirname="verisk",
        output_filename="verisk_ep_summary.long.csv",
    ),
    "risklink": EpSummaryVendorConfig(
        vendor="risklink",
        source_dirname="risklink",
        output_filename="rms_ep_summary.long.csv",
    ),
}


def ep_summary_vendor_names() -> list[str]:
    return list(EP_SUMMARY_VENDOR_CONFIGS)


def get_ep_summary_vendor_config(vendor: str) -> EpSummaryVendorConfig:
    try:
        return EP_SUMMARY_VENDOR_CONFIGS[vendor]
    except KeyError as exc:
        known_vendors = ", ".join(ep_summary_vendor_names())
        raise ValueError(
            f"unknown EP summary vendor {vendor!r}; expected one of: {known_vendors}"
        ) from exc


def scan_ep_summary_csvs(data_root: Path | str, vendor: str) -> list[Path]:
    config = get_ep_summary_vendor_config(vendor)
    return sorted(
        (
            path
            for path in config.source_dir(data_root).glob("*.csv")
            if not path.name.endswith(".long.csv")
        ),
        key=lambda path: path.name.lower(),
    )


def build_ep_summary_from_wide_csv(csv_path: Path | str, vendor: str) -> pl.DataFrame:
    csv_path = Path(csv_path)
    frame = pl.read_csv(csv_path, infer_schema=False)
    frame = _apply_source_aliases(frame)
    _validate_required_columns(frame, csv_path)

    metric_columns = _metric_columns(frame.columns)
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
    status_callback: Callable[[str], None] | None = None,
) -> Path:
    config = get_ep_summary_vendor_config(vendor)
    if status_callback is not None:
        status_callback("Reading CSV...")
    frame = build_ep_summary_from_wide_csv(csv_path, vendor)
    if status_callback is not None:
        status_callback("Writing canonical long CSV...")
    return _write_ep_summary(frame, config.output_path(data_root))


def generate_ep_summaries(data_root: Path | str = "data") -> list[Path]:
    data_root = Path(data_root)
    output_paths: list[Path] = []
    for vendor in EP_SUMMARY_VENDOR_CONFIGS:
        source_files = scan_ep_summary_csvs(data_root, vendor)
        if not source_files:
            raise FileNotFoundError(
                f"No source CSV files found in {get_ep_summary_vendor_config(vendor).source_dir(data_root)}."
            )
        output_paths.append(generate_vendor_ep_summary(data_root, vendor, source_files[0]))
    return output_paths


def _write_ep_summary(frame: pl.DataFrame, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    logger.info("writing output=%s", output_path, extra={"event": "ep_summary_write_start", "path": output_path})
    frame.write_csv(output_path)
    elapsed_seconds = time.perf_counter() - started
    logger.info(
        "wrote output=%s rows=%d elapsed=%.2fs",
        output_path,
        frame.height,
        elapsed_seconds,
        extra={"event": "ep_summary_write", "path": output_path, "rows": frame.height, "elapsed_seconds": elapsed_seconds},
    )
    return output_path


def _apply_source_aliases(frame: pl.DataFrame) -> pl.DataFrame:
    for alias, canonical in SOURCE_ALIASES.items():
        if canonical not in frame.columns and alias in frame.columns:
            frame = frame.rename({alias: canonical})
    return frame


def _validate_required_columns(frame: pl.DataFrame, csv_path: Path) -> None:
    missing_columns = [column for column in REQUIRED_SOURCE_COLUMNS if column not in frame.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"{csv_path} is missing required EP summary columns: {missing}")


def _metric_columns(columns: list[str]) -> list[str]:
    return [column for column in columns if METRIC_COLUMN_PATTERN.fullmatch(column)]


def _parse_metric_column(metric_name: str) -> dict[str, Any]:
    match = METRIC_COLUMN_PATTERN.fullmatch(metric_name)
    if match is None:
        raise ValueError(f"unsupported EP metric column: {metric_name}")

    ep_type = match.group(1).upper()
    return_period = int(float(match.group(2)))
    if ep_type == "AAL":
        return_period = 0
    return {"metric": metric_name, "ep_type": ep_type, "return_period": return_period}
