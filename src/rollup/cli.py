from __future__ import annotations

from argparse import ArgumentParser, Namespace
from dataclasses import replace
from pathlib import Path
from collections.abc import Sequence

from rollup.api import RollupRunResult, run_rollup
from rollup.config import RollupConfig, load_config
from rollup.logging import configure_console_logging


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(prog="rollup", description="Local rollup test runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="run the rollup pipeline locally")
    run_parser.add_argument("--data-root", type=Path, default=Path("data"))
    run_parser.add_argument("--output-root", type=Path, default=Path("output"))
    run_parser.add_argument("--config-path", type=Path, default=None)
    run_parser.add_argument("--no-analysis", action="store_false", dest="write_analysis")
    run_parser.add_argument("--no-stage-outputs", action="store_true")
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
    config = stage_output_config(args.config_path) if args.no_stage_outputs else None
    configure_console_logging(args.log_level, log_file=log_file)
    result = run_rollup(
        args.data_root,
        args.output_root,
        config_path=None if config is not None else args.config_path,
        config=config,
        write_analysis=args.write_analysis,
        log_file=log_file,
    )
    print_success_summary(result, log_file)
    return 0


def stage_output_config(config_path: Path | None) -> RollupConfig:
    config = load_config(config_path)
    return replace(config, outputs=replace(config.outputs, write_stage_outputs=False))


def print_success_summary(result: RollupRunResult, log_file: Path) -> None:
    print("Rollup complete")
    print(f"  data root: {result.data_root}")
    print(f"  output root: {result.output_root}")
    print(f"  log file: {log_file}")
    print(f"  marts dir: {result.outputs.marts_dir}")
    print(f"  combined mart: {result.outputs.mts_combined}")
    print(f"  wide mart: {result.outputs.mts_wide}")
    print(f"  dialsup mart: {result.outputs.mts_dialsup}")
    print(f"  event validation: {result.outputs.event_validation}")
    print(f"  analysis report: {result.ep_report_path or '(disabled)'}")
    print(f"  stage outputs: {result.outputs.stage_dir or '(disabled)'}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)
