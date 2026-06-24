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
    convert_ep_summaries as convert_all_ep_summaries,
    convert_ep_summary as convert_single_ep_summary,
)
from rollup.logging import temporary_file_logging
from rollup.pipeline import run

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RollupOutputPaths:
    mts_combined: Path
    mts_wide: Path
    mts_dialsup: Path
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


def run_rollup(
    data_root: str | Path = "data",
    output_root: str | Path = "output",
    *,
    config_path: str | Path | None = None,
    config: RollupConfig | None = None,
    write_analysis: bool = True,
    log_file: str | Path | None = None,
) -> RollupRunResult:
    with temporary_file_logging(log_file):
        data_root = Path(data_root)
        output_root = Path(output_root)
        config = config or load_config(config_path)
        started = time.perf_counter()
        logger.info("start rollup data_root=%s output_root=%s", data_root, output_root)
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


def build_ep_report(
    output_root: str | Path = "output", *, config_path: str | Path | None = None
) -> Path:
    return write_ep_report(output_root, config_path=config_path)


def convert_ep_summary(
    input_csv: str | Path,
    vendor: str,
    *,
    output_csv: str | Path | None = None,
) -> pl.DataFrame:
    return convert_single_ep_summary(input_csv, vendor, output_csv=output_csv)


def convert_ep_summaries(data_root: str | Path = "data") -> list[Path]:
    return convert_all_ep_summaries(data_root)


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
