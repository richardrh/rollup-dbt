from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from contextlib import contextmanager
import logging
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
from typing import TypeVar

import polars as pl

from rollup.api import (
    RollupValidationError,
    RollupValidationResult,
    build_ep_report,
    generate_ep_summary,
    run_rollup,
    validate_rollup_inputs,
)
from rollup.ep_summary_generator import (
    ep_summary_vendor_names,
    get_ep_summary_vendor_config,
    scan_ep_summary_csvs,
)
from rollup import resources as rollup_resources
from rollup.sql import check_sql_connection


DEFAULT_CONFIG_PATH = Path("rollup.local.toml")


def _add_config_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        type=Path,
        default=argparse.SUPPRESS,
        help="Path to rollup local TOML config (default: rollup.local.toml).",
    )


T = TypeVar("T")

_USER_FACING_EP_SUMMARY_ERRORS = (
    FileNotFoundError,
    KeyError,
    OSError,
    UnicodeDecodeError,
    ValueError,
    pl.exceptions.PolarsError,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rollup")
    parser.add_argument(
        "--data-root",
        default="data",
        help="Root data directory containing schema.yaml files and inputs.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Console logging level.",
    )
    parser.add_argument(
        "--output-root",
        default="output",
        help="Root output directory for generated pipeline artifacts.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Path to rollup local TOML config (default: rollup.local.toml).",
    )

    subcommands = parser.add_subparsers(dest="command", required=True)

    validate_parser = subcommands.add_parser(
        "validate",
        help="Validate configured inputs and print a validation report.",
    )
    validate_parser.add_argument(
        "--report-dir",
        type=Path,
        help="Directory to write validation report CSV files.",
    )

    subcommands.add_parser(
        "analyze",
        aliases=["analyse"],
        help="Build EP analysis CSV from pipeline outputs.",
    )

    cleanup_parser = subcommands.add_parser(
        "cleanup",
        help="Delete generated pipeline output files.",
    )
    cleanup_parser.add_argument(
        "--yes",
        action="store_true",
        help="Actually delete generated output files; default is dry run.",
    )

    ep_summary_parser = subcommands.add_parser(
        "generate-ep-summaries",
        help="Generate one canonical long EP summary CSV from a selected source wide CSV file.",
        description=(
            "Interactively select a vendor and discovered source wide CSV file, or pass "
            "--vendor, --csv, and --yes for non-interactive automation."
        ),
    )
    ep_summary_parser.add_argument(
        "--vendor",
        choices=ep_summary_vendor_names(),
        help="EP summary vendor to generate.",
    )
    ep_summary_parser.add_argument(
        "--csv",
        type=Path,
        help=(
            "Source canonical wide CSV path. Relative filenames are also resolved inside "
            "data/ep_summaries/<vendor>/."
        ),
    )
    ep_summary_parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Skip overwrite confirmation for automation.",
    )

    sql_check_parser = subcommands.add_parser(
        "sql-check",
        aliases=["test-sql"],
        help="Check configured SQL Server connectivity.",
    )
    _add_config_argument(sql_check_parser)

    run_parser = subcommands.add_parser(
        "run",
        help="Run the pipeline.",
    )
    run_parser.add_argument(
        "--debug",
        action="store_true",
        help="Write intermediate frames to output/debug.",
    )

    docs_parser = subcommands.add_parser(
        "docs",
        help="Serve the local documentation site.",
    )
    docs_parser.add_argument(
        "--host",
        default="localhost",
        help="Host interface for the docs server.",
    )
    docs_parser.add_argument(
        "--port",
        default=4322,
        type=int,
        help="Port for the docs server.",
    )

    return parser


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
        force=True,
    )


ValidationReports = RollupValidationResult


def collect_validation_reports(data_root: Path) -> ValidationReports:
    return validate_rollup_inputs(data_root)


def print_validation_reports(reports: ValidationReports) -> None:
    with pl.Config(
        tbl_cols=-1,
        tbl_rows=-1,
        tbl_width_chars=1000,
        fmt_str_lengths=1000,
    ):
        print("Validation report")
        print(reports.validation_report)
        print("\nModelled LOB/peril anti-join report")
        print(reports.coverage_report)
        print("\nYLT loss validation summary")
        print(reports.ylt_loss_report)
        print("\nInput YLT AAL by LOB/peril summary")
        print(reports.input_ylt_aal_report)


_VALIDATION_CSV_REPORTS = {
    "validation_report.csv": "validation_report",
    "modelled_lob_peril_anti_join_report.csv": "coverage_report",
    "ylt_loss_validation_summary.csv": "ylt_loss_report",
    "input_ylt_aal_by_lob_peril_summary.csv": "input_ylt_aal_report",
}


def write_validation_csv_reports(reports: ValidationReports, report_dir: Path) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    for filename, attribute_name in _VALIDATION_CSV_REPORTS.items():
        report = getattr(reports, attribute_name)
        report.write_csv(report_dir / filename)


def validation_exit_code(reports: ValidationReports) -> int:
    return 0 if reports.is_valid else 1


def validate_command(data_root: Path, *, report_dir: Path | None = None) -> int:
    reports = collect_validation_reports(data_root)
    print_validation_reports(reports)
    if report_dir is not None:
        try:
            write_validation_csv_reports(reports, report_dir)
        except Exception as exc:
            print(
                f"Failed to write validation CSV reports to {report_dir}: {exc}",
                file=sys.stderr,
            )
            return 1
        print(f"Validation CSV reports written to {report_dir}")
    return validation_exit_code(reports)


def run_command(
    data_root: Path,
    *,
    output_root: Path,
    debug: bool = False,
) -> int:
    try:
        result = run_rollup(
            data_root,
            output_root=output_root,
            debug=debug,
            validation_callback=print_validation_reports,
        )
    except RollupValidationError:
        return 1
    if debug:
        print(f"Debug frames written to {output_root / 'debug'}")

    if result.ep_report_path is not None:
        print(f"Analysis report written to {result.ep_report_path}")

    return 0


def analyze_command(output_root: Path) -> int:
    output_path = build_ep_report(output_root)
    print(f"Analysis report written to {output_path}")
    return 0


def cleanup_paths(output_root: Path) -> list[Path]:
    paths = sorted((output_root / "marts").glob("*.parquet"))
    paths.extend(
        path
        for path in [
            output_root / "mts_tbl_ylt_combined_all_factors.parquet",
            output_root / "mts_tbl_ylt_dialsup.parquet",
            output_root / "mts_event_validation.parquet",
        ]
        if path.is_file()
    )
    return paths


def cleanup_command(output_root: Path, *, yes: bool = False) -> int:
    paths = cleanup_paths(output_root)
    if not yes:
        print(f"Would delete {len(paths)} generated output file(s):")
        for path in paths:
            print(path)
        print("Pass --yes to delete these files.")
        return 0

    for path in paths:
        path.unlink(missing_ok=True)
    print(f"Deleted {len(paths)} generated output file(s).")
    return 0


def sql_check_command(config_path: Path = DEFAULT_CONFIG_PATH) -> int:
    result = check_sql_connection(config_path)
    print(f"SQL check {result.status}: {result.message}")
    return 0 if result.status == "OK" else 1


@contextmanager
def _working_directory(path: Path):
    previous_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous_cwd)


def _run_zensical(args: Sequence[str]) -> int:
    try:
        import zensical.main
    except ModuleNotFoundError:
        print(
            "Could not import Zensical. Install dev dependencies or use the PyInstaller build that bundles docs support.",
            file=sys.stderr,
        )
        return 1

    result = zensical.main.cli.main(
        args=list(args),
        prog_name="zensical",
        standalone_mode=False,
    )
    return int(result or 0)


@contextmanager
def _docs_runtime_project(docs_dir: Path, config_file: Path):
    if not rollup_resources.is_frozen():
        yield rollup_resources.resource_root(), config_file
        return

    with tempfile.TemporaryDirectory(prefix="rollup-docs-") as temp_dir:
        runtime_root = Path(temp_dir)
        runtime_docs_dir = runtime_root / "docs"
        runtime_config_file = runtime_root / "zensical.toml"
        shutil.copytree(docs_dir, runtime_docs_dir)
        shutil.copy2(config_file, runtime_config_file)
        yield runtime_root, runtime_config_file



def _run_docs_foreground(
    *,
    docs_dir: Path,
    config_file: Path,
    host: str,
    port: int,
    zensical_runner: Callable[[Sequence[str]], int | None] | None,
) -> int:
    print(f"Docs available at http://{host}:{port}/")
    runner = zensical_runner or _run_zensical
    args = [
        "serve",
        "--config-file",
        str(config_file),
        "--dev-addr",
        f"{host}:{port}",
    ]
    with _docs_runtime_project(docs_dir, config_file) as (runtime_root, runtime_config_file):
        args[2] = str(runtime_config_file)
        with _working_directory(runtime_root):
            result = runner(args)
    return int(result or 0)


def docs_command(
    *,
    host: str = "localhost",
    port: int = 4322,
    zensical_runner: Callable[[Sequence[str]], int | None] | None = None,
) -> int:
    docs_dir = rollup_resources.docs_dir()
    if not docs_dir.is_dir():
        print(
            f"Documentation source directory was not found: {docs_dir}",
            file=sys.stderr,
        )
        return 1
    config_file = rollup_resources.zensical_config_path()
    if not config_file.is_file():
        print(
            f"Zensical configuration file was not found: {config_file}",
            file=sys.stderr,
        )
        return 1

    return _run_docs_foreground(
        docs_dir=docs_dir,
        config_file=config_file,
        host=host,
        port=port,
        zensical_runner=zensical_runner,
    )


def _prompt_numbered_option(
    prompt: str,
    options: Sequence[T],
    display: Callable[[T], str] = str,
) -> T:
    while True:
        print(prompt)
        for index, option in enumerate(options, start=1):
            print(f"  {index}. {display(option)}")
        selection = input("Enter number: ").strip()
        if selection.isdecimal():
            index = int(selection)
            if 1 <= index <= len(options):
                return options[index - 1]
        print(f"Invalid selection. Enter a number from 1 to {len(options)}.")


def _confirm_overwrite(output_path: Path) -> bool:
    while True:
        response = input(f"Overwrite {output_path}? [y/N]: ").strip().lower()
        if response == "":
            return False
        if response in {"y", "yes"}:
            return True
        if response in {"n", "no"}:
            return False
        print("Please answer y or n.")


def _resolve_csv_path(data_root: Path, vendor: str, csv: Path) -> Path:
    if csv.is_absolute() or csv.is_file():
        return csv
    candidate = get_ep_summary_vendor_config(vendor).source_dir(data_root) / csv
    if candidate.is_file():
        return candidate
    return csv


def _sorted_distinct_values(frame: pl.DataFrame, column: str) -> list[str]:
    return sorted(str(value) for value in frame.get_column(column).drop_nulls().unique())


def _format_values(values: Sequence[str]) -> str:
    return ", ".join(values) if values else "none"


def _format_ep_type_counts(frame: pl.DataFrame) -> str:
    counts = frame.group_by("ep_type").len(name="count").sort("ep_type")
    values = [f"{ep_type}={count}" for ep_type, count in counts.iter_rows()]
    return ", ".join(values) if values else "none"


def _format_return_period_range(frame: pl.DataFrame) -> str:
    if frame.is_empty():
        return "n/a"
    return_periods = frame.get_column("return_period").drop_nulls()
    if return_periods.is_empty():
        return "n/a"
    return f"{return_periods.min()}-{return_periods.max()}"


def _print_ep_summary_overview(output_path: Path) -> None:
    frame = pl.read_csv(output_path)
    modelled_pair_count = frame.select(["modelled_lob", "modelled_peril"]).unique().height
    print("EP summary overview:")
    print(f"  Rows: {frame.height}")
    print(f"  Columns ({len(frame.columns)}): {', '.join(frame.columns)}")
    print(f"  Vendors: {_format_values(_sorted_distinct_values(frame, 'vendor'))}")
    print(f"  EP type counts: {_format_ep_type_counts(frame)}")
    print(f"  Modelled LOB/peril pairs: {modelled_pair_count}")
    print(f"  Return period range: {_format_return_period_range(frame)}")


def generate_ep_summaries_command(
    data_root: Path,
    *,
    vendor: str | None = None,
    csv: Path | None = None,
    yes: bool = False,
) -> int:
    try:
        selected_vendor = vendor or _prompt_numbered_option(
            "Select EP summary vendor:",
            ep_summary_vendor_names(),
        )

        try:
            config = get_ep_summary_vendor_config(selected_vendor)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 1

        if csv is None:
            csv_files = scan_ep_summary_csvs(data_root, selected_vendor)
            if not csv_files:
                print(
                    f"No source .csv files found in {config.source_dir(data_root)}.",
                    file=sys.stderr,
                )
                return 1
            csv_path = _prompt_numbered_option(
                "Select source wide CSV:",
                csv_files,
                lambda path: path.name,
            )
        else:
            csv_path = _resolve_csv_path(data_root, selected_vendor, csv)

        if not csv_path.is_file():
            print(f"CSV file not found: {csv_path}", file=sys.stderr)
            return 1

        output_path = config.output_path(data_root)
        if output_path.exists() and not yes and not _confirm_overwrite(output_path):
            print("EP summary generation cancelled; no files overwritten.")
            return 0

        print("EP summary generation:")
        print(f"  Vendor: {selected_vendor}")
        print(f"  CSV: {csv_path}")
        print(f"  Output: {output_path}")
        started = time.perf_counter()
        output_path = generate_ep_summary(
            data_root,
            selected_vendor,
            csv_path,
            status_callback=print,
        )
        elapsed_seconds = time.perf_counter() - started
    except EOFError:
        print("Input ended before EP summary generation could continue.", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("EP summary generation cancelled; no files overwritten.")
        return 130
    except _USER_FACING_EP_SUMMARY_ERRORS as exc:
        print(f"EP summary generation failed: {exc}", file=sys.stderr)
        return 1

    print(f"Done in {elapsed_seconds:.2f}s. EP summary written to {output_path}")
    _print_ep_summary_overview(output_path)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    data_root = Path(args.data_root)
    output_root = Path(args.output_root)
    config_path = Path(args.config)

    if args.command == "validate":
        return validate_command(data_root, report_dir=args.report_dir)
    if args.command == "run":
        return run_command(
            data_root,
            output_root=output_root,
            debug=args.debug,
        )
    if args.command in {"analyze", "analyse"}:
        return analyze_command(output_root)
    if args.command == "cleanup":
        return cleanup_command(output_root, yes=args.yes)
    if args.command in {"sql-check", "test-sql"}:
        return sql_check_command(config_path)
    if args.command == "generate-ep-summaries":
        return generate_ep_summaries_command(
            data_root,
            vendor=args.vendor,
            csv=args.csv,
            yes=args.yes,
        )
    if args.command == "docs":
        return docs_command(host=args.host, port=args.port)

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
