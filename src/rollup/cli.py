from __future__ import annotations

from argparse import ArgumentParser, Namespace
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
import logging
import sys

from rollup.api import RollupRunResult, RollupValidationError, run_rollup
from rollup.config import RollupConfig, load_config
from rollup.logging import configure_console_logging

logger = logging.getLogger(__name__)


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(prog="rollup", description="Local rollup test runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="run the rollup pipeline locally")
    run_parser.add_argument("--data-root", type=Path, default=Path("data"))
    run_parser.add_argument("--output-root", type=Path, default=Path("output"))
    run_parser.add_argument("--config-path", type=Path, default=None)
    run_parser.add_argument("--no-analysis", action="store_false", dest="write_analysis")
    run_parser.add_argument("--no-stage-outputs", action="store_true")
    run_parser.add_argument("--target-currency", type=str.upper, default=None)
    run_parser.add_argument("--duckdb", action="store_true", help="write a DuckDB export")
    run_parser.add_argument("--duckdb-file", type=Path, default=None, help="DuckDB output file path")
    run_parser.add_argument(
        "--log-level",
        type=str.upper,
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default="INFO",
    )
    run_parser.add_argument("--log-file", type=Path, default=None)
    run_parser.set_defaults(func=run_command, write_analysis=True)

    return parser


def run_command(args: Namespace) -> int:
    log_file = args.log_file or args.output_root / "rollup.log"
    config = override_config(args)
    configure_console_logging(args.log_level, log_file=log_file)
    try:
        result = run_rollup(
            args.data_root,
            args.output_root,
            config_path=None if config is not None else args.config_path,
            config=config,
            write_analysis=args.write_analysis,
            log_file=log_file,
        )
    except RollupValidationError as exc:
        message = validation_failure_message(exc)
        logger.error(message)
        print(message, file=sys.stderr)
        return 1
    print_success_summary(result, log_file)
    return 0


def override_config(args: Namespace) -> RollupConfig | None:
    if (
        not args.no_stage_outputs
        and args.target_currency is None
        and not args.duckdb
        and args.duckdb_file is None
    ):
        return None
    config = load_config(args.config_path)
    if args.no_stage_outputs:
        config = replace(config, outputs=replace(config.outputs, write_stage_outputs=False))
    if args.target_currency is not None:
        config = replace(config, fx=replace(config.fx, target_currency=args.target_currency))
    if args.duckdb or args.duckdb_file is not None:
        duckdb_file = config.outputs.duckdb_file
        if args.duckdb_file is not None:
            duckdb_file = str(args.duckdb_file.expanduser().resolve(strict=False))
        config = replace(config, outputs=replace(config.outputs, write_duckdb=True, duckdb_file=duckdb_file))
    return config


def print_success_summary(result: RollupRunResult, log_file: Path) -> None:
    marts_dir = result.outputs.marts_dir
    mart_count = _parquet_count(marts_dir)

    print("Rollup complete")
    print(f"  data root: {_display_path(result.data_root)}")
    print(f"  output root: {_display_path(result.output_root)} ({_exists_status(result.output_root)})")
    print(f"  log file: {_display_path(log_file)} ({_exists_status(log_file)})")
    print(f"  marts dir: {_display_path(marts_dir)} ({_exists_status(marts_dir)}, {_parquet_label(mart_count)})")
    print(f"  combined mart: {_display_path(result.outputs.mts_combined)}")
    print(f"  wide mart: {_display_path(result.outputs.mts_wide)}")
    print(f"  dialsup mart: {_display_path(result.outputs.mts_dialsup)}")
    print(f"  event validation: {_display_path(result.outputs.event_validation)}")
    _print_duckdb_summary(result.outputs.duckdb_file)
    _print_analysis_summary(result.ep_report_path)
    _print_stage_summary(result.outputs.stage_dir)


def validation_failure_message(exc: RollupValidationError) -> str:
    report = exc.validation.validation_report
    invalid = report.filter(~report["valid"]) if "valid" in report.columns else report
    if invalid.height == 0:
        invalid = report

    lines = ["Input validation failed"]
    for row in invalid.iter_rows(named=True):
        source_group = row.get("source_group")
        error = row.get("error")
        if source_group is not None:
            lines.append(f"source_group={source_group}")
        if error is not None:
            lines.append(f"error={error}")
    return "\n".join(lines)


def _print_duckdb_summary(duckdb_file: Path | None) -> None:
    if duckdb_file is None:
        print("  duckdb: (disabled)")
        return
    print(f"  duckdb: {_display_path(duckdb_file)} ({_exists_status(duckdb_file)})")


def _print_analysis_summary(ep_report_path: Path | None) -> None:
    if ep_report_path is None:
        print("  analysis report: (disabled)")
        return

    status = "exists" if ep_report_path.is_file() else "missing"
    print(f"  analysis report: {_display_path(ep_report_path)} ({status})")
    if status == "missing":
        print(f"  WARNING: analysis report missing: {_display_path(ep_report_path)}")


def _print_stage_summary(stage_dir: Path | None) -> None:
    if stage_dir is None:
        print("  stage outputs: (disabled)")
        return

    staging_dir = stage_dir / "staging"
    intermediate_dir = stage_dir / "intermediate"
    staging_count = _parquet_count(staging_dir)
    intermediate_count = _parquet_count(intermediate_dir)

    print(f"  stage outputs: {_display_path(stage_dir)} ({_exists_status(stage_dir)})")
    print(f"    staging: {_display_path(staging_dir)} ({_parquet_label(staging_count)})")
    print(f"    intermediate: {_display_path(intermediate_dir)} ({_parquet_label(intermediate_count)})")

    warnings = _stage_warnings("staging", staging_dir, staging_count) + _stage_warnings(
        "intermediate",
        intermediate_dir,
        intermediate_count,
    )
    if warnings:
        print(f"  WARNING: stage outputs incomplete: {', '.join(warnings)}")


def _stage_warnings(name: str, directory: Path, parquet_count: int) -> list[str]:
    if not directory.exists():
        return [f"{name} directory missing"]
    if parquet_count == 0:
        return [f"{name} parquet files missing"]
    return []


def _display_path(path: Path) -> str:
    return str(path.expanduser().resolve(strict=False))


def _exists_status(path: Path) -> str:
    return "exists" if path.exists() else "missing"


def _parquet_count(directory: Path) -> int:
    if not directory.exists():
        return 0
    return sum(1 for path in directory.glob("*.parquet") if path.is_file())


def _parquet_label(count: int) -> str:
    suffix = "file" if count == 1 else "files"
    return f"{count} parquet {suffix}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)
