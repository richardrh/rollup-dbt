from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from rollup.analysis import write_ep_report
from rollup.pipeline import (
    load_validated_ep_summary_frames,
    load_validated_seed_frames,
    load_validated_ylt_frames,
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

    run_parser = subcommands.add_parser(
        "run",
        help="Run the pipeline. Currently validates inputs first.",
    )
    run_parser.add_argument(
        "--debug",
        action="store_true",
        help="Write intermediate frames to data/output/debug.",
    )

    return parser


def validate_command(data_root: Path) -> int:
    seed_result = load_validated_seed_frames(data_root)
    ylt_result = load_validated_ylt_frames(data_root)
    ep_summary_result = load_validated_ep_summary_frames(data_root)

    report_parts = [
        seed_result.report.with_columns(pl.lit("seeds").alias("source_group")),
        ylt_result.report.with_columns(pl.lit("ylt").alias("source_group")),
        ep_summary_result.report.with_columns(pl.lit("ep_summaries").alias("source_group")),
    ]
    report_parts = [
        part.with_columns(pl.col("error").cast(pl.String))
        for part in report_parts
    ]

    report = pl.concat(
        report_parts,
        how="diagonal",
    )
    ylt_loss_report = ylt_loss_validation_summary(data_root)

    with pl.Config(
        tbl_cols=-1,
        tbl_rows=-1,
        tbl_width_chars=1000,
        fmt_str_lengths=1000,
    ):
        print("Validation report")
        print(report)
        print("\nYLT loss validation summary")
        print(ylt_loss_report)

    invalid_count = report.filter(~pl.col("valid")).height
    return 1 if invalid_count else 0


def run_command(data_root: Path, *, debug: bool = False) -> int:
    exit_code = validate_command(data_root)
    if exit_code:
        return exit_code

    run(data_root, debug=debug)
    if debug:
        print(f"Debug frames written to {data_root / 'output' / 'debug'}")
    return 0


def analyze_command(data_root: Path) -> int:
    output_path = write_ep_report(data_root)
    print(f"Analysis report written to {output_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    data_root = Path(args.data_root)

    if args.command == "validate":
        return validate_command(data_root)
    if args.command == "run":
        return run_command(data_root, debug=args.debug)
    if args.command in {"analyze", "analyse"}:
        return analyze_command(data_root)

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
