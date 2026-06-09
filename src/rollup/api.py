from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable
import logging
import time

import polars as pl
from pandera.errors import SchemaError, SchemaErrors

from rollup.analysis import write_ep_report
from rollup.config import RollupConfig, load_config
from rollup.duckdb_export import export_duckdb
from rollup.ep_summary_generator import write_ep_summaries as write_all_ep_summaries
from rollup.ep_summary_generator import write_vendor_ep_summary
from rollup.logging import temporary_file_logging
from rollup.pipeline import run
from rollup.staging import RollupInputValidationFailure, load_sources

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RollupOutputPaths:
    mts_combined: Path
    mts_wide: Path
    mts_dialsup: Path
    event_validation: Path
    marts_dir: Path
    mart_files: tuple[Path, ...]
    stage_dir: Path | None = None
    duckdb_file: Path | None = None


@dataclass(frozen=True)
class RollupRunResult:
    data_root: Path
    output_root: Path
    outputs: RollupOutputPaths
    ep_report_path: Path | None


@dataclass(frozen=True)
class RollupValidationResult:
    data_root: Path
    is_valid: bool
    validation_report: pl.DataFrame

    def raise_for_errors(self) -> None:
        if not self.is_valid:
            raise RollupValidationError(self)


class RollupValidationError(ValueError):
    def __init__(self, validation: RollupValidationResult) -> None:
        self.validation = validation
        errors = validation.validation_report.filter(~pl.col("valid"))
        super().__init__(
            f"rollup input validation failed with {errors.height} error(s)"
        )


def run_rollup(
    data_root: str | Path = "data",
    output_root: str | Path = "output",
    *,
    config_path: str | Path | None = None,
    config: RollupConfig | None = None,
    write_analysis: bool = True,
    validation_callback: Callable[[RollupValidationResult], None] | None = None,
    log_file: str | Path | None = None,
) -> RollupRunResult:
    with temporary_file_logging(log_file):
        data_root = Path(data_root)
        output_root = Path(output_root)
        config = config or load_config(config_path)
        started = time.perf_counter()
        logger.info("start rollup data_root=%s output_root=%s", data_root, output_root)
        validation = validate_rollup_inputs(data_root)
        if validation_callback is not None:
            validation_callback(validation)
        validation.raise_for_errors()
        run(data_root, output_root=output_root, config=config)
        if config.outputs.write_duckdb:
            export_duckdb(data_root, output_root, config)
        ep_report_path = (
            write_ep_report(output_root, config=config) if write_analysis else None
        )
        logger.info(
            "done rollup output_root=%s elapsed=%.2fs",
            output_root,
            time.perf_counter() - started,
        )
        return RollupRunResult(
            data_root=data_root,
            output_root=output_root,
            outputs=collect_output_paths(output_root, config=config),
            ep_report_path=ep_report_path,
        )


def validate_rollup_inputs(data_root: str | Path = "data") -> RollupValidationResult:
    data_root = Path(data_root)
    try:
        load_sources(data_root)
    except (
        SchemaError,
        SchemaErrors,
        FileNotFoundError,
        RollupInputValidationFailure,
    ) as exc:
        report = pl.DataFrame(
            [
                {
                    "source_group": "runtime_schema_guard",
                    "valid": False,
                    "error": str(exc),
                }
            ]
        )
        return RollupValidationResult(
            data_root=data_root, is_valid=False, validation_report=report
        )
    report = pl.DataFrame(
        [{"source_group": "runtime_schema_guard", "valid": True, "error": None}]
    )
    return RollupValidationResult(
        data_root=data_root, is_valid=True, validation_report=report
    )


def build_ep_report(
    output_root: str | Path = "output", *, config_path: str | Path | None = None
) -> Path:
    return write_ep_report(output_root, config_path=config_path)


def write_ep_summary(
    data_root: str | Path,
    vendor: str,
    csv_path: str | Path,
) -> Path:
    return write_vendor_ep_summary(data_root, vendor, csv_path)


def write_ep_summaries(data_root: str | Path = "data") -> list[Path]:
    return write_all_ep_summaries(data_root)


def collect_output_paths(
    output_root: str | Path = "output",
    *,
    config: RollupConfig | None = None,
) -> RollupOutputPaths:
    config = config or load_config()
    output_root = Path(output_root)
    marts_dir = config.outputs.marts_path(output_root)
    return RollupOutputPaths(
        mts_combined=marts_dir / config.outputs.combined_file,
        mts_wide=marts_dir / config.outputs.wide_file,
        mts_dialsup=marts_dir / config.outputs.dialsup_file,
        event_validation=marts_dir / config.outputs.event_validation_file,
        marts_dir=marts_dir,
        mart_files=tuple(sorted(marts_dir.glob("*.parquet")))
        if marts_dir.exists()
        else (),
        stage_dir=output_root / config.outputs.stage_output_dir
        if config.outputs.write_stage_outputs
        else None,
        duckdb_file=config.outputs.duckdb_path(output_root)
        if config.outputs.write_duckdb
        else None,
    )
