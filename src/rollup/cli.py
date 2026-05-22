from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from dataclasses import dataclass
import logging
from pathlib import Path
import subprocess
import sys
from typing import TypeVar

import polars as pl

from rollup.analysis import write_ep_report
from rollup.ep_summary_generator import (
    ep_summary_vendor_names,
    generate_vendor_ep_summary,
    get_ep_summary_vendor_config,
    scan_ep_summary_workbooks,
)
from rollup.pipeline import (
    PipelineValidationInputs,
    empty_input_ylt_aal_by_lob_peril_summary,
    input_ylt_aal_by_lob_peril_summary,
    load_pipeline_validation_inputs,
    run,
    ylt_loss_validation_summary,
)


T = TypeVar("T")


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

    ep_summary_parser = subcommands.add_parser(
        "generate-ep-summaries",
        help="Generate one canonical long EP summary CSV from a selected source XLSX file.",
        description=(
            "Interactively select a vendor and discovered source XLSX file, or pass "
            "--vendor, --xlsx, and --yes for non-interactive automation."
        ),
    )
    ep_summary_parser.add_argument(
        "--vendor",
        choices=ep_summary_vendor_names(),
        help="EP summary vendor to generate.",
    )
    ep_summary_parser.add_argument(
        "--xlsx",
        type=Path,
        help=(
            "Source XLSX workbook path. Relative filenames are also resolved inside "
            "data/ep_summaries/<vendor>/."
        ),
    )
    ep_summary_parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Skip overwrite confirmation for automation.",
    )

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
        default="127.0.0.1",
        help="Host interface for the docs server.",
    )
    docs_parser.add_argument(
        "--port",
        default=8000,
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


@dataclass(frozen=True)
class ValidationReports:
    inputs: PipelineValidationInputs
    report: pl.DataFrame
    coverage_report: pl.DataFrame
    ylt_loss_report: pl.DataFrame
    input_ylt_aal_report: pl.DataFrame


def _validation_report(inputs: PipelineValidationInputs) -> pl.DataFrame:
    report_parts = [
        inputs.seeds.report.with_columns(pl.lit("seeds").alias("source_group")),
        inputs.ylts.report.with_columns(pl.lit("ylt").alias("source_group")),
        inputs.ep_summaries.report.with_columns(pl.lit("ep_summaries").alias("source_group")),
    ]
    return pl.concat(
        [part.with_columns(pl.col("error").cast(pl.String)) for part in report_parts],
        how="diagonal",
    )


def collect_validation_reports(data_root: Path) -> ValidationReports:
    inputs = load_pipeline_validation_inputs(data_root)
    report = _validation_report(inputs)
    try:
        ylt_loss_report = ylt_loss_validation_summary(data_root)
    except Exception as exc:
        ylt_loss_report = pl.DataFrame(
            [{"valid": False, "error": f"YLT loss validation summary failed: {exc}"}]
        )
    try:
        input_ylt_aal_report = input_ylt_aal_by_lob_peril_summary(inputs)
    except Exception:
        input_ylt_aal_report = empty_input_ylt_aal_by_lob_peril_summary()
    return ValidationReports(
        inputs=inputs,
        report=report,
        coverage_report=inputs.coverage_report,
        ylt_loss_report=ylt_loss_report,
        input_ylt_aal_report=input_ylt_aal_report,
    )


def print_validation_reports(reports: ValidationReports) -> None:
    with pl.Config(
        tbl_cols=-1,
        tbl_rows=-1,
        tbl_width_chars=1000,
        fmt_str_lengths=1000,
    ):
        print("Validation report")
        print(reports.report)
        print("\nModelled LOB/peril anti-join report")
        print(reports.coverage_report)
        print("\nYLT loss validation summary")
        print(reports.ylt_loss_report)
        print("\nInput YLT AAL by LOB/peril summary")
        print(reports.input_ylt_aal_report)


_VALIDATION_CSV_REPORTS = {
    "validation_report.csv": "report",
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
    invalid_count = reports.report.filter(~pl.col("valid")).height
    coverage_error_count = reports.coverage_report.filter(
        pl.col("severity") == "error"
    ).height
    return 1 if invalid_count or coverage_error_count else 0


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
    reports = collect_validation_reports(data_root)
    print_validation_reports(reports)
    if validation_exit_code(reports):
        return 1

    run(
        data_root,
        output_root=output_root,
        debug=debug,
        validation_inputs=reports.inputs,
    )
    if debug:
        print(f"Debug frames written to {output_root / 'debug'}")
    return 0


def analyze_command(output_root: Path) -> int:
    output_path = write_ep_report(output_root)
    print(f"Analysis report written to {output_path}")
    return 0


def docs_command(*, host: str = "127.0.0.1", port: int = 8000) -> int:
    docs_dir = Path("docs")
    if not docs_dir.is_dir():
        print(
            "Documentation source directory 'docs/' was not found. "
            "Create docs/index.md before serving docs.",
            file=sys.stderr,
        )
        return 1

    url = f"http://{host}:{port}/"
    print(f"Docs available at {url}")
    try:
        return subprocess.call(
            [
                "zensical",
                "serve",
                "--config-file",
                "zensical.toml",
                "--dev-addr",
                f"{host}:{port}",
            ]
        )
    except FileNotFoundError:
        print(
            "Could not find the 'zensical' executable. Install dev dependencies or run with `uv run rollup docs`.",
            file=sys.stderr,
        )
        return 1


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


def _resolve_xlsx_path(data_root: Path, vendor: str, xlsx: Path) -> Path:
    if xlsx.is_absolute() or xlsx.is_file():
        return xlsx
    candidate = get_ep_summary_vendor_config(vendor).source_dir(data_root) / xlsx
    if candidate.is_file():
        return candidate
    return xlsx


def generate_ep_summaries_command(
    data_root: Path,
    *,
    vendor: str | None = None,
    xlsx: Path | None = None,
    yes: bool = False,
) -> int:
    selected_vendor = vendor or _prompt_numbered_option(
        "Select EP summary vendor:",
        ep_summary_vendor_names(),
    )

    try:
        config = get_ep_summary_vendor_config(selected_vendor)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if xlsx is None:
        workbooks = scan_ep_summary_workbooks(data_root, selected_vendor)
        if not workbooks:
            print(
                f"No .xlsx files found in {config.source_dir(data_root)}.",
                file=sys.stderr,
            )
            return 1
        workbook_path = _prompt_numbered_option(
            "Select source XLSX workbook:",
            workbooks,
            lambda path: path.name,
        )
    else:
        workbook_path = _resolve_xlsx_path(data_root, selected_vendor, xlsx)

    if not workbook_path.is_file():
        print(f"XLSX workbook not found: {workbook_path}", file=sys.stderr)
        return 1

    output_path = config.output_path(data_root)
    if not yes and not _confirm_overwrite(output_path):
        print("EP summary generation cancelled; no files overwritten.")
        return 0

    output_path = generate_vendor_ep_summary(data_root, selected_vendor, workbook_path)
    print(f"EP summary written to {output_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    data_root = Path(args.data_root)
    output_root = Path(args.output_root)

    if args.command == "validate":
        return validate_command(data_root, report_dir=args.report_dir)
    if args.command == "run":
        return run_command(data_root, output_root=output_root, debug=args.debug)
    if args.command in {"analyze", "analyse"}:
        return analyze_command(output_root)
    if args.command == "generate-ep-summaries":
        return generate_ep_summaries_command(
            data_root,
            vendor=args.vendor,
            xlsx=args.xlsx,
            yes=args.yes,
        )
    if args.command == "docs":
        return docs_command(host=args.host, port=args.port)

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
