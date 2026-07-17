from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import time

from rollup.analysis import write_ep_report
from rollup.config import RollupConfig, read_config
from rollup.logging import LogFormat, temporary_file_logging, validate_log_format
from rollup.output_contract import (
    COMBINED_YLT_FILE,
    DEBUG_DIR,
    DIALSUP_YLT_FILE,
    EVENT_VALIDATION_FILE,
    MARTS_DIR,
    WIDE_YLT_FILE,
)
from rollup.pipeline import run as run_pipeline
from rollup.writers import duckdb_export

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RollupRunResult:
    data_root: Path
    output_root: Path
    mts_combined: Path
    mts_wide: Path
    mts_dialsup: Path
    event_validation: Path
    marts_dir: Path
    mart_files: tuple[Path, ...]
    debug_dir: Path | None = None
    duckdb_file: Path | None = None
    ep_report_path: Path | None = None


def run_rollup(
    data_root: str | Path = "data",
    output_root: str | Path = "output",
    *,
    config_path: str | Path | None = None,
    config: RollupConfig | None = None,
    debug: bool = False,
    write_analysis: bool = True,
    log_file: str | Path | None = None,
    log_format: LogFormat | None = None,
) -> RollupRunResult:
    data_root = Path(data_root)
    output_root = Path(output_root)
    config = config or read_config(config_path)
    selected_log_format = validate_log_format(log_format or config.logging.format)
    with temporary_file_logging(log_file, log_format=selected_log_format):
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
            run_pipeline(data_root, output_root=output_root, debug=debug, config=config)
            ep_report_path = (
                write_ep_report(output_root, config=config) if write_analysis else None
            )
            duckdb_file = (
                duckdb_export.write(data_root, output_root, config)
                if config.outputs.write_duckdb
                else None
            )
            marts_dir = output_root / MARTS_DIR
            result = RollupRunResult(
                data_root=data_root,
                output_root=output_root,
                mts_combined=output_root / COMBINED_YLT_FILE,
                mts_wide=output_root / WIDE_YLT_FILE,
                mts_dialsup=output_root / DIALSUP_YLT_FILE,
                event_validation=output_root / EVENT_VALIDATION_FILE,
                marts_dir=marts_dir,
                mart_files=tuple(sorted(marts_dir.glob("*.parquet")))
                if marts_dir.exists()
                else (),
                debug_dir=output_root / DEBUG_DIR if debug else None,
                duckdb_file=duckdb_file,
                ep_report_path=ep_report_path,
            )
        except Exception:
            elapsed_seconds = time.perf_counter() - started
            logger.exception(
                "failed rollup elapsed_seconds=%.2f",
                elapsed_seconds,
                extra={"event": "rollup_failed", "elapsed_seconds": elapsed_seconds},
            )
            raise
        elapsed_seconds = time.perf_counter() - started
        logger.info(
            "done rollup output_root=%s elapsed_seconds=%.2f",
            output_root,
            elapsed_seconds,
            extra={
                "event": "rollup_done",
                "output_root": output_root,
                "elapsed_seconds": elapsed_seconds,
            },
        )
        return result
