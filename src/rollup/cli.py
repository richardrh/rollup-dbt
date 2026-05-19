from __future__ import annotations

import argparse
from dataclasses import dataclass
import logging
from pathlib import Path
import subprocess
import sys

import polars as pl

from rollup.analysis import write_ep_report
from rollup.ep_summary_generator import generate_ep_summaries
from rollup.pipeline import (
    PipelineValidationInputs,
    load_pipeline_validation_inputs,
    run,
    ylt_loss_validation_summary,
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

    subcommands = parser.add_subparsers(dest="command", required=True)

    subcommands.add_parser(
        "validate",
        help="Validate configured inputs and print a validation report.",
    )

    subcommands.add_parser(
        "analyze",
        aliases=["analyse"],
        help="Build EP analysis CSV from pipeline outputs.",
    )

    subcommands.add_parser(
        "generate-ep-summaries",
        help="Generate canonical long EP summary CSVs from source XLSX files.",
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
    return ValidationReports(
        inputs=inputs,
        report=report,
        coverage_report=inputs.coverage_report,
        ylt_loss_report=ylt_loss_report,
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


def validation_exit_code(reports: ValidationReports) -> int:
    invalid_count = reports.report.filter(~pl.col("valid")).height
    coverage_error_count = reports.coverage_report.filter(
        pl.col("severity") == "error"
    ).height
    return 1 if invalid_count or coverage_error_count else 0


def validate_command(data_root: Path) -> int:
    reports = collect_validation_reports(data_root)
    print_validation_reports(reports)
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


def generate_ep_summaries_command(data_root: Path) -> int:
    output_paths = generate_ep_summaries(data_root)
    for output_path in output_paths:
        print(f"EP summary written to {output_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    data_root = Path(args.data_root)
    output_root = Path(args.output_root)

    if args.command == "validate":
        return validate_command(data_root)
    if args.command == "run":
        return run_command(data_root, output_root=output_root, debug=args.debug)
    if args.command in {"analyze", "analyse"}:
        return analyze_command(output_root)
    if args.command == "generate-ep-summaries":
        return generate_ep_summaries_command(data_root)
    if args.command == "docs":
        return docs_command(host=args.host, port=args.port)

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
