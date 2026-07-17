from __future__ import annotations

from argparse import ArgumentParser, Namespace, SUPPRESS
from collections.abc import Sequence
from dataclasses import replace
from dataclasses import dataclass
from pathlib import Path
import logging
import sys
import tempfile

import polars as pl

from rollup.api import run_rollup
from rollup.config import RollupConfig, read_config
from rollup.ep_summary_generator import (
    EP_SUMMARY_OUTPUT_FILENAMES,
    generate_vendor_ep_summary,
    scan_ep_summary_csvs,
)
from rollup.logging import configure_console_logging, validate_log_format
from rollup.output_contract import (
    COMBINED_YLT_FILE,
    DIALSUP_YLT_FILE,
    EVENT_VALIDATION_FILE,
    MARTS_DIR,
    WIDE_YLT_FILE,
)
from rollup import validation

logger = logging.getLogger(__name__)
EXPECTED_VALIDATION_EXCEPTIONS = (
    FileNotFoundError,
    OSError,
    ValueError,
    KeyError,
    pl.exceptions.PolarsError,
)


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        prog="rollup",
        description="Catastrophe model rollup pipeline for validated local datasets",
    )
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--output-root", type=Path, default=Path("output"))
    parser.add_argument("--log-file", type=Path, default=None)
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="run the rollup pipeline locally")
    run_parser.add_argument("--data-root", type=Path, default=SUPPRESS)
    run_parser.add_argument("--output-root", type=Path, default=SUPPRESS)
    run_parser.add_argument("--config", type=Path, default=SUPPRESS)
    run_parser.add_argument("--debug", action="store_true")
    run_parser.add_argument(
        "--no-analysis", action="store_false", dest="write_analysis"
    )
    duckdb_group = run_parser.add_mutually_exclusive_group()
    duckdb_group.add_argument(
        "--duckdb", action="store_true", help="write a DuckDB export"
    )
    duckdb_group.add_argument(
        "--no-duckdb", action="store_true", help="disable DuckDB export"
    )
    run_parser.add_argument(
        "--duckdb-file", type=Path, default=None, help="DuckDB output file path"
    )
    run_parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default="INFO",
    )
    run_parser.add_argument("--log-file", type=Path, default=SUPPRESS)
    run_parser.add_argument(
        "--log-format",
        choices=("text", "jsonl"),
        default=None,
        help="log output format (default: [logging].format from config, jsonl if unset)",
    )

    ep_parser = subparsers.add_parser(
        "generate-ep-summaries",
        help="convert wide vendor EP summary CSVs to canonical long CSVs",
    )
    ep_parser.add_argument("--data-root", type=Path, default=SUPPRESS)
    ep_parser.add_argument(
        "--vendor", choices=tuple(EP_SUMMARY_OUTPUT_FILENAMES), default=None
    )
    ep_parser.add_argument("--csv", type=Path, default=None)

    validate_parser = subparsers.add_parser("validate", help="validate rollup inputs")
    validate_parser.add_argument("--data-root", type=Path, default=SUPPRESS)
    validate_parser.add_argument("--report-dir", type=Path, default=None)

    cleanup_parser = subparsers.add_parser("cleanup", help="remove generated outputs")
    cleanup_parser.add_argument("--yes", action="store_true")
    return parser


def run_command(args: Namespace) -> int:
    log_file = args.log_file or args.output_root / "rollup.log"
    config = override_config(args)
    logging_config = config or read_config(args.config)
    log_format = validate_log_format(args.log_format or logging_config.logging.format)
    configure_console_logging(args.log_level, log_file=log_file, log_format=log_format)
    run_rollup(
        args.data_root,
        args.output_root,
        config_path=None if config is not None else args.config,
        config=config,
        debug=args.debug,
        write_analysis=args.write_analysis,
        log_file=log_file,
        log_format=log_format,
    )
    return 0


def generate_ep_summaries_command(args: Namespace) -> int:
    try:
        if args.vendor is None and args.csv is None:
            vendor = _prompt_choice(
                "Select EP summary vendor:", list(EP_SUMMARY_OUTPUT_FILENAMES)
            )
            source_dir = args.data_root / "ep_summaries" / vendor
            paths = scan_ep_summary_csvs(args.data_root, vendor)
            if not paths:
                raise FileNotFoundError(f"No source CSV files found in {source_dir}.")
            csv_path = _prompt_choice("Select source wide CSV:", paths)
            output_path = source_dir / EP_SUMMARY_OUTPUT_FILENAMES[vendor]
            if (
                output_path.exists()
                and input(f"Overwrite {output_path}? [y/N] ").lower() != "y"
            ):
                print("EP summary generation skipped; existing output preserved.")
                return 0
            generate_vendor_ep_summary(args.data_root, vendor, csv_path)
        elif args.vendor is not None and args.csv is not None:
            csv_path = (
                args.csv
                if args.csv.is_absolute()
                else args.data_root / "ep_summaries" / args.vendor / args.csv
            )
            if not csv_path.exists():
                raise FileNotFoundError(f"CSV file not found: {args.csv}")
            generate_vendor_ep_summary(args.data_root, args.vendor, csv_path)
        else:
            raise ValueError("--vendor and --csv must be passed together")
    except EOFError:
        print(
            "Input ended before EP summary generation could continue.", file=sys.stderr
        )
        return 1
    except KeyboardInterrupt:
        print("EP summary generation cancelled; no files overwritten.")
        return 130
    except (FileNotFoundError, ValueError) as exc:
        print(f"EP summary generation failed: {exc}", file=sys.stderr)
        return 1
    return 0


def _prompt_choice(prompt: str, options: list):
    print(prompt)
    for index, option in enumerate(options, start=1):
        print(f"  {index}. {option}")
    selected = int(input("> "))
    if selected < 1 or selected > len(options):
        raise ValueError(f"selection must be between 1 and {len(options)}")
    return options[selected - 1]


def override_config(args: Namespace) -> RollupConfig | None:
    if args.no_duckdb and args.duckdb_file is not None:
        raise ValueError("--no-duckdb cannot be combined with --duckdb-file")
    if (
        not args.duckdb
        and not args.no_duckdb
        and args.duckdb_file is None
        and args.log_format is None
    ):
        return None
    config = read_config(args.config)
    if args.no_duckdb:
        config = replace(
            config,
            outputs=replace(config.outputs, write_duckdb=False, duckdb_file=None),
        )
    if args.duckdb or args.duckdb_file is not None:
        duckdb_file = config.outputs.duckdb_file
        if args.duckdb_file is not None:
            duckdb_file = str(args.duckdb_file.expanduser().resolve(strict=False))
        config = replace(
            config,
            outputs=replace(config.outputs, write_duckdb=True, duckdb_file=duckdb_file),
        )
    if args.log_format is not None:
        config = replace(
            config, logging=replace(config.logging, format=args.log_format)
        )
    return config


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "run":
        if args.no_duckdb and args.duckdb_file is not None:
            parser.error("--no-duckdb cannot be combined with --duckdb-file")
        return run_command(args)
    if args.command == "generate-ep-summaries":
        return generate_ep_summaries_command(args)
    if args.command == "validate":
        return validate_command(args.data_root, report_dir=args.report_dir)
    if args.command == "cleanup":
        return cleanup_command(args.output_root, yes=args.yes)
    parser.error(f"unknown command: {args.command}")
    return 2


@dataclass(frozen=True)
class ValidationReports:
    data_root: Path
    is_valid: bool
    coverage_report: pl.DataFrame
    input_ylt_aal_report: pl.DataFrame


def collect_validation_reports(data_root: str | Path) -> ValidationReports:
    data_root = Path(data_root)
    inputs = validation.inspect_inputs(data_root)
    coverage_report = inputs.coverage_report
    aal_report = validation.input_ylt_aal_by_lob_peril_summary(inputs)
    has_errors = coverage_report.filter(pl.col("severity") == "error").height > 0
    is_valid = not has_errors
    return ValidationReports(data_root, is_valid, coverage_report, aal_report)


def write_validation_csv_reports(reports: ValidationReports, report_dir: Path) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    validation_report_frames = (
        ("modelled_lob_peril_anti_join_report.csv", reports.coverage_report),
        ("input_ylt_aal_by_lob_peril_summary.csv", reports.input_ylt_aal_report),
    )
    with tempfile.TemporaryDirectory(
        prefix="rollup-validation-", dir=report_dir.parent
    ) as staging_dir_name:
        staging_dir = Path(staging_dir_name)
        staged_paths = []
        for filename, frame in validation_report_frames:
            staged_path = staging_dir / filename
            frame.write_csv(staged_path)
            staged_paths.append((staged_path, report_dir / filename))
        for staged_path, output_path in staged_paths:
            staged_path.replace(output_path)


def validate_command(data_root: str | Path, *, report_dir: Path | None = None) -> int:
    try:
        reports = collect_validation_reports(data_root)
    except EXPECTED_VALIDATION_EXCEPTIONS as exc:
        print(f"Validation failed: {exc}", file=sys.stderr)
        return 1
    if report_dir is not None:
        try:
            write_validation_csv_reports(reports, report_dir)
        except OSError as exc:
            print(f"Failed to write validation CSV reports: {exc}", file=sys.stderr)
            return 1
    return 0 if reports.is_valid else 1


def cleanup_command(output_root: Path, *, yes: bool = False) -> int:
    targets = [
        output_root / COMBINED_YLT_FILE,
        output_root / WIDE_YLT_FILE,
        output_root / DIALSUP_YLT_FILE,
        output_root / EVENT_VALIDATION_FILE,
    ]
    targets.extend(
        (output_root / MARTS_DIR).glob("*.parquet")
        if (output_root / MARTS_DIR).exists()
        else []
    )
    if yes:
        for path in targets:
            path.unlink(missing_ok=True)
    else:
        for path in targets:
            print(f"Would delete: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
