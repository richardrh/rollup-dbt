from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
import logging
import time


from rollup.analysis import write_ep_report
from rollup.ep_summary_generator import generate_vendor_ep_summary
from rollup.logging import temporary_file_logging
from rollup.pipeline import (
    run,
    )

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
    debug: bool = False,
    validate: bool = True,
    write_analysis: bool = True,
    validation_callback: Callable[[RollupValidationResult], None] | None = None,
    log_file: str | Path | None = None,
) -> RollupRunResult:
    with temporary_file_logging(log_file):
        data_root = Path(data_root)
        output_root = Path(output_root)
        started = time.perf_counter()
        logger.info(
            "start rollup data_root=%s output_root=%s debug=%s validate=%s write_analysis=%s",
            data_root,
            output_root,
            debug,
            validate,
            write_analysis,
        )
        try:
            validation_bundle = _collect_validation(data_root)
            validation = validation_bundle.result
            if validation_callback is not None:
                validation_callback(validation)
            if validate:
                validation.raise_for_errors()

            run(
                data_root,
                output_root=output_root,
                debug=debug,
                validation_inputs=validation_bundle.inputs,
            )
            ep_report_path = write_ep_report(output_root) if write_analysis else None
            result = RollupRunResult(
                data_root=data_root,
                output_root=output_root,
                validation=validation,
                outputs=collect_output_paths(output_root, debug=debug),
                ep_report_path=ep_report_path,
            )
        except Exception:
            logger.exception("failed rollup elapsed=%.2fs", time.perf_counter() - started)
            raise
        logger.info("done rollup output_root=%s elapsed=%.2fs", output_root, time.perf_counter() - started)
        return result


def build_ep_report(output_root: str | Path = "output") -> Path:
    return write_ep_report(output_root)


def generate_ep_summary(
    data_root: str | Path,
    vendor: str,
    csv_path: str | Path,
    *,
    status_callback: Callable[[str], None] | None = None,
) -> Path:
    return generate_vendor_ep_summary(
        data_root,
        vendor,
        csv_path,
        status_callback=status_callback,
    )


def collect_output_paths(
    output_root: str | Path = "output",
    *,
    debug: bool = False,
) -> RollupOutputPaths:
    output_root = Path(output_root)
    marts_dir = output_root / "marts"
    return RollupOutputPaths(
        mts_combined=output_root / "mts_tbl_ylt_combined_all_factors.parquet",
        mts_wide=output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet",
        mts_dialsup=output_root / "mts_tbl_ylt_dialsup.parquet",
        event_validation=output_root / "mts_event_validation.parquet",
        marts_dir=marts_dir,
        mart_files=tuple(sorted(marts_dir.glob("*.parquet"))),
        debug_dir=output_root / "debug" if debug else None,
    )

