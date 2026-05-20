from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import subprocess
import sys

import polars as pl

from rollup.analysis import write_ep_report
from rollup.ep_summary_generator import generate_ep_summaries
from rollup.pipeline import (
    PipelineValidationInputs,
    empty_input_ylt_aal_by_lob_peril_summary,
    input_ylt_aal_by_lob_peril_summary,
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
    docs_parser.add_argument(
        "--foreground",
        action="store_true",
        help="Run the docs server in the foreground instead of the default background mode.",
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


DOCS_TMP_DIR = Path(".tmp")
DOCS_LOG_PATH = DOCS_TMP_DIR / "rollup-docs.log"
DOCS_PID_PATH = DOCS_TMP_DIR / "rollup-docs.pid"
DOCS_STARTUP_CHECK_SECONDS = 0.25


@dataclass(frozen=True)
class DocsServerState:
    pid: int
    host: str
    port: int


def _docs_server_command(host: str, port: int) -> list[str]:
    return [
        "zensical",
        "serve",
        "--config-file",
        "zensical.toml",
        "--dev-addr",
        f"{host}:{port}",
    ]


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _read_live_docs_state(pid_path: Path, *, host: str, port: int) -> DocsServerState | None:
    try:
        raw_state = json.loads(pid_path.read_text())
        state = DocsServerState(
            pid=int(raw_state["pid"]),
            host=str(raw_state["host"]),
            port=int(raw_state["port"]),
        )
    except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None

    if state.host != host or state.port != port:
        return None
    if state.pid <= 0 or not _pid_is_alive(state.pid):
        pid_path.unlink(missing_ok=True)
        return None
    return state


def _write_docs_state(pid_path: Path, *, pid: int, host: str, port: int) -> None:
    pid_path.write_text(json.dumps({"pid": pid, "host": host, "port": port}) + "\n")


def _exited_immediately(process: subprocess.Popen[bytes], *, log_path: Path) -> int | None:
    try:
        status = process.wait(timeout=DOCS_STARTUP_CHECK_SECONDS)
    except subprocess.TimeoutExpired:
        return None
    print(
        f"Docs server exited immediately with status {status}. See logs: {log_path}",
        file=sys.stderr,
    )
    return status if status != 0 else 1


def _print_background_docs_details(*, url: str, pid: int, log_path: Path) -> None:
    print(f"Docs available at {url}")
    print(f"Docs server running in background with PID {pid}")
    print(f"Logs: {log_path}")
    print(f"Stop with: kill {pid}")


def docs_command(*, host: str = "127.0.0.1", port: int = 8000, foreground: bool = False) -> int:
    docs_dir = Path("docs")
    if not docs_dir.is_dir():
        print(
            "Documentation source directory 'docs/' was not found. "
            "Create docs/index.md before serving docs.",
            file=sys.stderr,
        )
        return 1

    url = f"http://{host}:{port}/"
    command = _docs_server_command(host, port)

    if foreground:
        print(f"Docs available at {url}")
        try:
            return subprocess.call(command)
        except FileNotFoundError:
            print(
                "Could not find the 'zensical' executable. Install dev dependencies or run with `uv run rollup docs`.",
                file=sys.stderr,
            )
            return 1

    live_state = _read_live_docs_state(DOCS_PID_PATH, host=host, port=port)
    if live_state is not None:
        _print_background_docs_details(url=url, pid=live_state.pid, log_path=DOCS_LOG_PATH)
        return 0

    DOCS_TMP_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with DOCS_LOG_PATH.open("ab") as log_file:
            process = subprocess.Popen(command, stdout=log_file, stderr=log_file)
    except FileNotFoundError:
        print(
            "Could not find the 'zensical' executable. Install dev dependencies or run with `uv run rollup docs`.",
            file=sys.stderr,
        )
        return 1
    startup_status = _exited_immediately(process, log_path=DOCS_LOG_PATH)
    if startup_status is not None:
        return startup_status
    _write_docs_state(DOCS_PID_PATH, pid=process.pid, host=host, port=port)
    _print_background_docs_details(url=url, pid=process.pid, log_path=DOCS_LOG_PATH)
    return 0


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
        return validate_command(data_root, report_dir=args.report_dir)
    if args.command == "run":
        return run_command(data_root, output_root=output_root, debug=args.debug)
    if args.command in {"analyze", "analyse"}:
        return analyze_command(output_root)
    if args.command == "generate-ep-summaries":
        return generate_ep_summaries_command(data_root)
    if args.command == "docs":
        return docs_command(host=args.host, port=args.port, foreground=args.foreground)

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
