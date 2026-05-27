from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from typing import TypeVar

import polars as pl

from rollup.analysis import write_ep_report
from rollup.ep_summary_generator import (
    ep_summary_vendor_names,
    generate_vendor_ep_summary,
    get_ep_summary_vendor_config,
    scan_ep_summary_csvs,
)
from rollup.pipeline import (
    PipelineValidationInputs,
    empty_input_ylt_aal_by_lob_peril_summary,
    input_ylt_aal_by_lob_peril_summary,
    load_pipeline_validation_inputs,
    run,
    ylt_loss_validation_summary,
)
from rollup import resources as rollup_resources
from rollup.sql import (
    check_sql_connection,
    push_mart_parquets_to_sql,
    require_working_sql_config,
)


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
    _add_config_argument(run_parser)
    run_parser.add_argument(
        "--debug",
        action="store_true",
        help="Write intermediate frames to output/debug.",
    )
    run_parser.add_argument(
        "--push-sql",
        action="store_true",
        help="Push output/marts/*.parquet to SQL Server after a successful run.",
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
    config_path: Path = DEFAULT_CONFIG_PATH,
    debug: bool = False,
    push_sql: bool = False,
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

    analysis_path = write_ep_report(output_root)
    print(f"Analysis report written to {analysis_path}")

    if push_sql:
        try:
            sql_config = require_working_sql_config(config_path)
            pushed_tables = push_mart_parquets_to_sql(output_root, sql_config)
        except Exception as exc:
            print(f"Failed to push marts to SQL Server: {exc}", file=sys.stderr)
            return 1
        print(f"Pushed {len(pushed_tables)} mart parquet file(s) to SQL Server")
    return 0


def analyze_command(output_root: Path) -> int:
    output_path = write_ep_report(output_root)
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


DOCS_TMP_DIR = Path(".tmp")
DOCS_LOG_PATH = DOCS_TMP_DIR / "rollup-docs.log"
DOCS_PID_PATH = DOCS_TMP_DIR / "rollup-docs.pid"
DOCS_STARTUP_CHECK_SECONDS = 0.25


@dataclass(frozen=True)
class DocsServerState:
    pid: int
    host: str
    port: int


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


def _docs_server_command(host: str, port: int) -> list[str]:
    command = [
        sys.executable,
        "docs",
        "--host",
        host,
        "--port",
        str(port),
        "--foreground",
    ]
    if not rollup_resources.is_frozen():
        command.insert(1, "-m")
        command.insert(2, "rollup.cli")
    return command


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _process_matches_docs_command(pid: int, command: list[str]) -> bool:
    try:
        raw_cmdline = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return False

    args = [part.decode(errors="ignore") for part in raw_cmdline.split(b"\0") if part]
    if not args:
        return False
    return all(expected in args for expected in command)


def _read_live_docs_state(
    pid_path: Path,
    *,
    host: str,
    port: int,
    command: list[str],
) -> DocsServerState | None:
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
    if (
        state.pid <= 0
        or not _pid_is_alive(state.pid)
        or not _process_matches_docs_command(state.pid, command)
    ):
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
    host: str = "127.0.0.1",
    port: int = 8000,
    foreground: bool = False,
    zensical_runner: Callable[[Sequence[str]], int | None] | None = None,
    process_factory: Callable[..., subprocess.Popen[bytes]] | None = None,
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

    url = f"http://{host}:{port}/"
    if foreground:
        return _run_docs_foreground(
            docs_dir=docs_dir,
            config_file=config_file,
            host=host,
            port=port,
            zensical_runner=zensical_runner,
        )

    command = _docs_server_command(host, port)
    process_factory = process_factory or subprocess.Popen
    live_state = _read_live_docs_state(DOCS_PID_PATH, host=host, port=port, command=command)
    if live_state is not None:
        _print_background_docs_details(url=url, pid=live_state.pid, log_path=DOCS_LOG_PATH)
        return 0

    DOCS_TMP_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with DOCS_LOG_PATH.open("ab") as log_file:
            process = process_factory(command, stdout=log_file, stderr=log_file)
    except FileNotFoundError:
        print(
            "Could not start the docs server process. Install dev dependencies or run with `uv run rollup docs --foreground`.",
            file=sys.stderr,
        )
        return 1

    startup_status = _exited_immediately(process, log_path=DOCS_LOG_PATH)
    if startup_status is not None:
        return startup_status
    _write_docs_state(DOCS_PID_PATH, pid=process.pid, host=host, port=port)
    _print_background_docs_details(url=url, pid=process.pid, log_path=DOCS_LOG_PATH)
    return 0


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
        output_path = generate_vendor_ep_summary(data_root, selected_vendor, csv_path, status_callback=print)
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
            config_path=config_path,
            debug=args.debug,
            push_sql=args.push_sql,
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
        return docs_command(host=args.host, port=args.port, foreground=args.foreground)

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
