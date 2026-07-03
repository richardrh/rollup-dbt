from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import time

import polars as pl

from rollup.analysis import write_ep_report
from rollup.config import RollupConfig, load_config
from rollup.duckdb_export import export_duckdb
from rollup.ep_summary_generator import (
    generate_ep_summaries as convert_all_ep_summaries,
    build_ep_summary_from_wide_csv as convert_single_ep_summary,
    generate_vendor_ep_summary,
)
from rollup.logging import LogFormat, temporary_file_logging
from rollup.pipeline import run

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RollupOutputPaths:
    mts_combined: Path
    mts_wide: Path
    mts_dialsup: Path
    event_validation: Path
    marts_dir: Path
    mart_files: tuple[Path, ...]
    debug_dir: Path | None = None
    duckdb_file: Path | None = None


@dataclass(frozen=True)
class RollupRunResult:
    data_root: Path
    output_root: Path
    outputs: RollupOutputPaths
    ep_report_path: Path | None


def run_rollup(
    data_root: str | Path = "data",
    output_root: str | Path = "output",
    *,
    config_path: str | Path | None = None,
    config: RollupConfig | None = None,
    debug: bool = False,
    validate: bool = True,
    write_analysis: bool = True,
    log_file: str | Path | None = None,
    log_format: LogFormat | None = None,
) -> RollupRunResult:
    del validate
    data_root = Path(data_root)
    output_root = Path(output_root)
    config = config or load_config(config_path)
    with temporary_file_logging(log_file, log_format=log_format or config.logging.format):
        started = time.perf_counter()
        logger.info(
            "start rollup data_root=%s output_root=%s debug=%s write_analysis=%s",
            data_root,
            output_root,
            debug,
            write_analysis,
            extra={
                "event": "rollup_start",
                "data_root": data_root,
                "output_root": output_root,
                "debug": debug,
                "write_analysis": write_analysis,
            },
        )
        try:
            run(data_root, output_root=output_root, debug=debug, config=config)
            ep_report_path = write_ep_report(output_root) if write_analysis else None
            duckdb_file = None
            if config.outputs.write_duckdb:
                duckdb_file = export_duckdb(data_root, output_root, config)
            result = RollupRunResult(
                data_root=data_root,
                output_root=output_root,
                outputs=collect_output_paths(output_root, debug=debug, config=config, duckdb_file=duckdb_file),
                ep_report_path=ep_report_path,
            )
        except Exception:
            elapsed_seconds = time.perf_counter() - started
            logger.exception(
                "failed rollup elapsed=%.2fs",
                elapsed_seconds,
                extra={"event": "rollup_failed", "elapsed_seconds": elapsed_seconds},
            )
            raise
        elapsed_seconds = time.perf_counter() - started
        logger.info(
            "done rollup output_root=%s elapsed=%.2fs",
            output_root,
            elapsed_seconds,
            extra={"event": "rollup_done", "output_root": output_root, "elapsed_seconds": elapsed_seconds},
        )
        return result


def build_ep_report(output_root: str | Path = "output") -> Path:
    return write_ep_report(output_root)


def convert_ep_summary(
    input_csv: str | Path,
    vendor: str,
    *,
    output_csv: str | Path | None = None,
) -> pl.DataFrame:
    frame = convert_single_ep_summary(input_csv, vendor)
    if output_csv is not None:
        Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
        frame.write_csv(output_csv)
    return frame


def convert_ep_summaries(data_root: str | Path = "data") -> list[Path]:
    return convert_all_ep_summaries(data_root)


def generate_ep_summary(data_root: str | Path, vendor: str, csv_path: str | Path, *, status_callback=None) -> Path:
    return generate_vendor_ep_summary(data_root, vendor, csv_path, status_callback=status_callback)


def collect_output_paths(
    output_root: str | Path = "output",
    *,
    debug: bool = False,
    config: RollupConfig | None = None,
    duckdb_file: Path | None = None,
) -> RollupOutputPaths:
    config = config or load_config()
    output_root = Path(output_root)
    marts_dir = output_root / config.outputs.marts_dir
    return RollupOutputPaths(
        mts_combined=output_root / config.outputs.combined_file,
        mts_wide=output_root / config.outputs.wide_file,
        mts_dialsup=output_root / config.outputs.dialsup_file,
        event_validation=output_root / "mts_event_validation.parquet",
        marts_dir=marts_dir,
        mart_files=tuple(sorted(marts_dir.glob("*.parquet"))) if marts_dir.exists() else (),
        debug_dir=output_root / "debug" if debug else None,
        duckdb_file=duckdb_file if duckdb_file is not None else (
            config.outputs.duckdb_path(output_root) if config.outputs.write_duckdb else None
        ),
    )
