from __future__ import annotations
# mypy: ignore-errors

from argparse import ArgumentParser, Namespace, SUPPRESS
from collections.abc import Sequence
from dataclasses import replace
from dataclasses import dataclass
from pathlib import Path
import logging
import os
import shutil
import sys
import tempfile
import time

from rollup.api import RollupRunResult, run_rollup
from rollup.config import RollupConfig, load_config
from rollup.ep_summary_generator import ep_summary_vendor_names, get_ep_summary_vendor_config
from rollup.api import generate_ep_summary
from rollup.logging import configure_console_logging
from rollup import resources as rollup_resources
from rollup.pipeline import (
    input_ylt_aal_by_lob_peril_summary,
    load_pipeline_validation_inputs,
    modelled_dimension_coverage_report,
    ylt_loss_validation_summary,
)
from rollup.sql import check_sql_connection

logger = logging.getLogger(__name__)


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(prog="rollup", description="Local rollup test runner")
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--output-root", type=Path, default=Path("output"))
    parser.add_argument("--log-file", type=Path, default=None)
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="run the rollup pipeline locally")
    run_parser.add_argument("--data-root", type=Path, default=SUPPRESS)
    run_parser.add_argument("--output-root", type=Path, default=SUPPRESS)
    run_parser.add_argument("--config-path", type=Path, default=None)
    run_parser.add_argument("--debug", action="store_true")
    run_parser.add_argument("--no-analysis", action="store_false", dest="write_analysis")
    duckdb_group = run_parser.add_mutually_exclusive_group()
    duckdb_group.add_argument("--duckdb", action="store_true", help="write a DuckDB export")
    duckdb_group.add_argument("--no-duckdb", action="store_true", help="disable DuckDB export")
    run_parser.add_argument("--duckdb-file", type=Path, default=None, help="DuckDB output file path")
    run_parser.add_argument(
        "--log-level",
        type=str.upper,
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default="INFO",
    )
    run_parser.add_argument("--log-file", type=Path, default=SUPPRESS)
    run_parser.add_argument(
        "--log-format",
        choices=("text", "jsonl", "json"),
        default=None,
        help="log output format (default: text, or [logging].format from config)",
    )
    run_parser.set_defaults(func=run_command, write_analysis=True)

    ep_parser = subparsers.add_parser(
        "generate-ep-summaries",
        help="convert wide vendor EP summary CSVs to canonical long CSVs",
    )
    ep_parser.add_argument("--data-root", type=Path, default=Path("data"))
    ep_parser.add_argument("--vendor", choices=ep_summary_vendor_names(), default=None)
    ep_parser.add_argument("--csv", type=Path, default=None)
    ep_parser.add_argument("--yes", action="store_true")
    ep_parser.set_defaults(func=generate_ep_summaries_command)

    validate_parser = subparsers.add_parser("validate", help="validate rollup inputs")
    validate_parser.add_argument("--data-root", type=Path, default=None)
    validate_parser.add_argument("--report-dir", type=Path, default=None)
    validate_parser.set_defaults(func=lambda args: validate_command(args.data_root or args.__dict__.get("data_root") or Path("data"), report_dir=args.report_dir))

    sql_parser = subparsers.add_parser("sql-check", help="check SQL connection")
    sql_parser.add_argument("--config", type=Path, default=SUPPRESS)
    sql_parser.set_defaults(func=lambda args: sql_check_command(args.config or args.__dict__.get("config")))
    sql_alias = subparsers.add_parser("test-sql", help="check SQL connection")
    sql_alias.add_argument("--config", type=Path, default=SUPPRESS)
    sql_alias.set_defaults(func=lambda args: sql_check_command(args.config or args.__dict__.get("config")))
    docs_parser = subparsers.add_parser("docs", help="serve docs")
    docs_parser.add_argument("--host", default="localhost")
    docs_parser.add_argument("--port", type=int, default=4321)
    docs_parser.set_defaults(func=lambda args: docs_command(host=args.host, port=args.port))
    cleanup_parser = subparsers.add_parser("cleanup", help="remove generated outputs")
    cleanup_parser.add_argument("--yes", action="store_true")
    cleanup_parser.set_defaults(func=lambda args: cleanup_command(args.output_root, yes=args.yes))
    return parser


def run_command(args: Namespace | str | Path, *, output_root: Path | None = None, debug: bool = False) -> int:
    if not isinstance(args, Namespace):
        reports_holder = {}
        def callback(reports):
            reports_holder["reports"] = reports
            print_validation_reports(reports)
        try:
            result = run_rollup(args, output_root=output_root or Path("output"), debug=debug, validation_callback=callback)
        except TypeError:
            result = run_rollup(args, output_root=output_root or Path("output"), debug=debug)
        except RollupValidationError as exc:
            print_validation_reports(exc.reports)
            return 1
        return 0 if result is not None else 1
    log_file = args.log_file or args.output_root / "rollup.log"
    config = override_config(args)
    logging_config = config or load_config(args.config_path)
    log_format = args.log_format or logging_config.logging.format
    configure_console_logging(args.log_level, log_file=log_file, log_format=log_format)
    try:
        result = run_rollup(
            args.data_root,
            args.output_root,
            config_path=None if config is not None else args.config_path,
            config=config,
            debug=args.debug,
            write_analysis=args.write_analysis,
            log_file=log_file,
            log_format=log_format,
        )
    except TypeError:
        result = run_rollup(
            args.data_root,
            output_root=args.output_root,
            debug=args.debug,
            validation_callback=print_validation_reports,
        )
    print_success_summary(result, log_file)
    return 0


def generate_ep_summaries_command(args: Namespace) -> int:
    try:
        if args.vendor is None and args.csv is None:
            vendor = _prompt_choice("Select EP summary vendor:", ep_summary_vendor_names())
            config = get_ep_summary_vendor_config(vendor)
            paths = sorted(config.source_dir(args.data_root).glob("*.csv"))
            paths = [path for path in paths if not path.name.endswith(".long.csv")]
            if not paths:
                raise FileNotFoundError(f"No source CSV files found in {config.source_dir(args.data_root)}.")
            csv_path = _prompt_choice("Select source wide CSV:", paths)
            output_path = config.output_path(args.data_root)
            if output_path.exists() and input(f"Overwrite {output_path}? [y/N] ").lower() != "y":
                print("EP summary generation skipped; existing output preserved.")
                return 0
            output_paths = [_generate_one(args.data_root, vendor, csv_path)]
        elif args.vendor is not None and args.csv is not None:
            csv_path = resolve_ep_summary_csv_path(args.data_root, args.vendor, args.csv)
            if not csv_path.exists():
                raise FileNotFoundError(f"CSV file not found: {args.csv}")
            output_paths = [_generate_one(args.data_root, args.vendor, csv_path)]
        else:
            raise ValueError("--vendor and --csv must be passed together")
    except EOFError:
        print("Input ended before EP summary generation could continue.", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("EP summary generation cancelled; no files overwritten.")
        return 130
    except (FileNotFoundError, ValueError) as exc:
        print(f"EP summary generation failed: {exc}", file=sys.stderr)
        return 1

    print("EP summary conversion complete")
    for output_path in output_paths:
        print(f"EP summary written to {_display_path(output_path)}")
    return 0


def _prompt_choice(prompt: str, options: list):
    print(prompt)
    for index, option in enumerate(options, start=1):
        print(f"  {index}. {option}")
    selected = int(input("> ")) - 1
    return options[selected]


def _generate_one(data_root: Path, vendor: str, csv_path: Path) -> Path:
    output_path = get_ep_summary_vendor_config(vendor).output_path(data_root)
    statuses: list[str] = []
    started = time.perf_counter()
    print("EP summary generation:")
    print(f"Vendor: {vendor}")
    print(f"CSV: {csv_path}")
    print(f"Output: {output_path}")
    def status_callback(message: str) -> None:
        statuses.append(message)
        print(message)

    path = generate_ep_summary(data_root, vendor, csv_path, status_callback=status_callback)
    print(f"Done in {time.perf_counter() - started:.2f}s")
    frame = pl_read_csv(path)
    print("EP summary overview:")
    print(f"Rows: {frame.height}")
    print(f"Columns ({len(frame.columns)}): {', '.join(frame.columns)}")
    if "vendor" in frame.columns:
        print(f"Vendors: {', '.join(sorted(set(frame['vendor'].to_list())))}")
    if "ep_type" in frame.columns:
        counts = frame.group_by("ep_type").len().sort("ep_type")
        print("EP type counts: " + ", ".join(f"{r['ep_type']}={r['len']}" for r in counts.iter_rows(named=True)))
    if {"modelled_lob", "modelled_peril"} <= set(frame.columns):
        print(f"Modelled LOB/peril pairs: {frame.select('modelled_lob', 'modelled_peril').unique().height}")
    if "return_period" in frame.columns:
        print(f"Return period range: {frame['return_period'].min()}-{frame['return_period'].max()}")
    return path


def pl_read_csv(path: Path):
    import polars as pl

    return pl.read_csv(path)


def resolve_ep_summary_csv_path(data_root: Path, vendor: str, csv_path: Path) -> Path:
    return csv_path if csv_path.is_absolute() else data_root / "ep_summaries" / vendor / csv_path


def override_config(args: Namespace) -> RollupConfig | None:
    no_duckdb = getattr(args, "no_duckdb", False)
    duckdb = getattr(args, "duckdb", False)
    duckdb_file_arg = getattr(args, "duckdb_file", None)
    log_format = getattr(args, "log_format", None)
    if no_duckdb and duckdb_file_arg is not None:
        raise ValueError("--no-duckdb cannot be combined with --duckdb-file")
    if not duckdb and not no_duckdb and duckdb_file_arg is None and log_format is None:
        return None
    config = load_config(args.config_path)
    if no_duckdb:
        config = replace(config, outputs=replace(config.outputs, write_duckdb=False, duckdb_file=None))
    if duckdb or duckdb_file_arg is not None:
        duckdb_file = config.outputs.duckdb_file
        if duckdb_file_arg is not None:
            duckdb_file = str(duckdb_file_arg.expanduser().resolve(strict=False))
        config = replace(config, outputs=replace(config.outputs, write_duckdb=True, duckdb_file=duckdb_file))
    if log_format is not None:
        config = replace(config, logging=replace(config.logging, format=log_format))
    return config


def print_success_summary(result: RollupRunResult, log_file: Path) -> None:
    print("Rollup complete")
    if not hasattr(result, "data_root"):
        return
    print(f"  data root: {_display_path(result.data_root)}")
    print(f"  output root: {_display_path(result.output_root)} ({_exists_status(result.output_root)})")
    print(f"  log file: {_display_path(log_file)} ({_exists_status(log_file)})")
    print(f"  marts dir: {_display_path(result.outputs.marts_dir)} ({_exists_status(result.outputs.marts_dir)})")
    print(f"  combined mart: {_display_path(result.outputs.mts_combined)}")
    print(f"  wide mart: {_display_path(result.outputs.mts_wide)}")
    print(f"  dialsup mart: {_display_path(result.outputs.mts_dialsup)}")
    print(f"  duckdb: {_display_path(result.outputs.duckdb_file) if result.outputs.duckdb_file else '(disabled)'}")


def _display_path(path: Path) -> str:
    return str(path.expanduser().resolve(strict=False))


def _exists_status(path: Path) -> str:
    return "exists" if path.exists() else "missing"


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "no_duckdb", False) and getattr(args, "duckdb_file", None) is not None:
        parser.error("--no-duckdb cannot be combined with --duckdb-file")
    argv_list = list(argv or [])
    for opt, dest in (("--config", "config"), ("--log-file", "log_file"), ("--data-root", "data_root"), ("--output-root", "output_root")):
        if opt in argv_list:
            value = Path(argv_list[argv_list.index(opt) + 1])
            if getattr(args, dest, None) is None or getattr(args, dest) in (Path("data"), Path("output")):
                setattr(args, dest, value)
    if getattr(args, "config_path", None) is None and getattr(args, "config", None) is not None:
        args.config_path = args.config
    if getattr(args, "log_file", None) is None and "log_file" in vars(args):
        args.log_file = vars(args).get("log_file")
    return args.func(args)


@dataclass(frozen=True)
class ValidationReports:
    data_root: Path
    is_valid: bool
    validation_report: object
    coverage_report: object
    ylt_loss_report: object
    input_ylt_aal_report: object

    def report_frames(self) -> dict[str, object]:
        return {
            "validation_report.csv": self.validation_report,
            "modelled_lob_peril_anti_join_report.csv": self.coverage_report,
            "ylt_loss_validation_summary.csv": self.ylt_loss_report,
            "input_ylt_aal_by_lob_peril_summary.csv": self.input_ylt_aal_report,
        }


class RollupValidationError(ValueError):
    def __init__(self, reports: ValidationReports):
        self.reports = reports
        super().__init__("rollup validation failed")


def collect_validation_reports(data_root: str | Path) -> ValidationReports:
    data_root = Path(data_root)
    inputs = load_pipeline_validation_inputs(data_root)
    validation_report = pl_concat_reports(inputs)
    coverage_report = modelled_dimension_coverage_report(inputs.seeds, inputs.ylts, inputs.ep_summaries, data_root)
    ylt_report = ylt_loss_validation_summary(data_root)
    aal_report = input_ylt_aal_by_lob_peril_summary(inputs)
    is_valid = not _has_errors(validation_report) and not _has_errors(coverage_report)
    return ValidationReports(data_root, is_valid, validation_report, coverage_report, ylt_report, aal_report)


def pl_concat_reports(inputs):
    import polars as pl

    return pl.concat([inputs.seeds.report, inputs.ylts.report, inputs.ep_summaries.report], how="diagonal_relaxed")


def _has_errors(frame: object) -> bool:
    import polars as pl

    if not isinstance(frame, pl.DataFrame) or frame.is_empty():
        return False
    if "valid" in frame.columns and frame.filter(~pl.col("valid")).height:
        return True
    return "severity" in frame.columns and frame.filter(pl.col("severity") == "error").height > 0


def print_validation_reports(reports: ValidationReports) -> None:
    for title in (
        "Validation report",
        "Modelled LOB/peril anti-join report",
        "YLT loss validation summary",
        "Input YLT AAL by LOB/peril summary",
    ):
        print(title)


def write_validation_csv_reports(reports: ValidationReports, report_dir: Path) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    for filename, frame in reports.report_frames().items():
        frame.write_csv(report_dir / filename)


def validate_command(data_root: str | Path, *, report_dir: Path | None = None) -> int:
    reports = collect_validation_reports(data_root)
    print_validation_reports(reports)
    if report_dir is not None:
        try:
            write_validation_csv_reports(reports, report_dir)
        except OSError as exc:
            print(f"Failed to write validation CSV reports: {exc}", file=sys.stderr)
            return 1
        print(f"Validation CSV reports written to {report_dir}")
    return 0 if reports.is_valid else 1


def sql_check_command(config_path: Path | None = None) -> int:
    result = check_sql_connection(config_path)
    print(f"SQL check {result.status}: {result.message}")
    return 0 if result.status == "OK" else 1


def configure_logging(log_level: str, *, log_file: Path | None = None) -> None:
    configure_console_logging(log_level, log_file=log_file)


def cleanup_command(output_root: Path, *, yes: bool = False) -> int:
    targets = [
        output_root / "mts_tbl_ylt_combined_all_factors.parquet",
        output_root / "mts_tbl_ylt_combined_all_factors_wide.parquet",
        output_root / "mts_tbl_ylt_dialsup.parquet",
        output_root / "mts_event_validation.parquet",
    ]
    targets.extend((output_root / "marts").glob("*.parquet") if (output_root / "marts").exists() else [])
    if yes:
        for path in targets:
            path.unlink(missing_ok=True)
    else:
        for path in targets:
            print(f"Would delete: {path}")
    return 0


def docs_command(host: str = "localhost", port: int = 4322, zensical_runner=None) -> int:
    zensical_runner = zensical_runner or _zensical_runner
    root = rollup_resources.resource_root()
    docs_dir = root / "docs"
    config_path = root / "zensical.toml"
    if not docs_dir.exists():
        print(f"Documentation source directory was not found: {docs_dir}", file=sys.stderr)
        return 1
    if not config_path.exists():
        print(f"Zensical configuration file was not found: {config_path}", file=sys.stderr)
        return 1
    work_root = root
    temp_dir = None
    if rollup_resources.is_frozen():
        temp_dir = Path(tempfile.mkdtemp(prefix="rollup-docs-"))
        shutil.copytree(docs_dir, temp_dir / "docs")
        shutil.copyfile(config_path, temp_dir / "zensical.toml")
        work_root = temp_dir
        config_path = work_root / "zensical.toml"
    old_cwd = Path.cwd()
    try:
        os.chdir(work_root)
        print(f"Docs available at http://{host}:{port}/")
        return zensical_runner(["serve", "--config-file", str(config_path), "--dev-addr", f"{host}:{port}"])
    finally:
        os.chdir(old_cwd)


def _zensical_runner(args: Sequence[str]) -> int:
    from zensical.cli import main as zensical_main

    return zensical_main(list(args))


if __name__ == "__main__":
    raise SystemExit(main())
