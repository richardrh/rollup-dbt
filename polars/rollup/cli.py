"""Command-line entry point.

Wired up two ways:

  * Console script (preferred):   `uv run rollup …`
  * Module form:                  `uv run python -m rollup …`

`rollup --help` lists every subcommand. The default flow (no subcommand)
runs the pipeline; `--dry-run` prints the plan and exits.

Subcommand handlers (`_cmd_*`) live in this file and stay deliberately
thin — they parse arguments, hand off to a stage / IO module, and shape
the exit code. Real work lives in `rollup.pipeline`, `rollup.io`, and
`rollup.stages`.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from rollup import config


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    config.setup_logging(args.log_level)

    handler = {
        "ep-summary-to-csv": _cmd_ep_summary_to_csv,
        "derive-blending":   _cmd_derive_blending,
    }.get(args.cmd, _cmd_run)
    return handler(args)


# --------------------------------------------------------------------------- #
# Parser                                                                      #
# --------------------------------------------------------------------------- #

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rollup",
        description="Polars rollup pipeline — RiskLink + Verisk YLTs → Hisco parquets.",
    )
    parser.add_argument(
        "--log-level", default=None, metavar="LEVEL",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="verbosity (default WARNING; also settable via ROLLUP_LOG env var)",
    )

    # Default-flow flags (no subcommand) — `rollup --yes` runs the pipeline,
    # `rollup --dry-run` prints the plan. Kept on the top-level parser so
    # bare `rollup ...` keeps working without a `run` subcommand.
    parser.add_argument(
        "-y", "--yes", action="store_true",
        help="skip the interactive y/N confirmation",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="print the plan and exit without running the pipeline",
    )
    parser.add_argument(
        "-d", "--dump-interim", action="store_true",
        help="also write audit_wide.parquet + audit_long.parquet to "
             "<output_dir>/debug/ for read-across verification",
    )

    sub = parser.add_subparsers(dest="cmd", metavar="<subcommand>")

    sub.add_parser(
        "ep-summary-to-csv",
        help="Convert wide EP-summary xlsx files under data/ep_summaries/{vendor}/ "
             "into long-format CSVs next to them.",
    )

    blend = sub.add_parser(
        "derive-blending",
        help="Derive blending_weights.csv from EP-summary long CSVs "
             "(run ep-summary-to-csv first).",
    )
    blend.add_argument(
        "--output", type=Path, default=None,
        help="Where to write the blending_weights CSV. "
             "Default: <seeds_dir>/vor/blending_weights.csv (overwrites the existing seed).",
    )

    return parser


# --------------------------------------------------------------------------- #
# Subcommand handlers                                                         #
# --------------------------------------------------------------------------- #

def _cmd_run(args: argparse.Namespace) -> int:
    """Default flow: build the plan, prompt (or not), run the pipeline."""
    from rollup.pipeline import run

    cfg  = config.resolve()
    plan = config.build_plan(cfg)

    if args.dry_run:
        if sys.stdout.isatty():
            config.print_plan(plan)
        else:
            print(config.format_plan(plan))
        return 0

    if not plan.all_seeds_ok:
        # On stderr we use plain text — colour codes leak in CI logs.
        print(config.format_plan(plan), file=sys.stderr)
        print("aborting: one or more seeds failed schema validation", file=sys.stderr)
        return 2

    if not config.confirm(plan, assume_yes=args.yes):
        print("aborted by user")
        return 1

    run(cfg, dump_interim=args.dump_interim)
    return 0


def _cmd_ep_summary_to_csv(args: argparse.Namespace) -> int:
    """Convert every xlsx under each vendor's ep_summary_dir into a long-CSV sibling."""
    from rollup.io.ep_summary import convert_ep_summaries_to_csv

    cfg = config.resolve()
    written: list[Path] = []
    for vendor in cfg.vendors:
        written.extend(
            convert_ep_summaries_to_csv(vendor.ep_summary_dir, vendor.name)
        )
    for p in written:
        print(f"wrote {p}")
    print(f"total: {len(written)} csv file(s)")
    return 0


def _cmd_derive_blending(args: argparse.Namespace) -> int:
    """Compute blending_weights.csv from the long-format EP CSVs."""
    from rollup.config import VendorName
    from rollup.seeds import load_all
    from rollup.stages.blending import derive_blending_weights

    cfg = config.resolve()
    output = args.output or (cfg.seeds_dir / "vor" / "blending_weights.csv")

    rl_csvs = sorted(cfg.vendor(VendorName.RISKLINK).ep_summary_dir.glob("*.long.csv"))
    vk_csvs = sorted(cfg.vendor(VendorName.VERISK).ep_summary_dir.glob("*.long.csv"))
    if not rl_csvs and not vk_csvs:
        print(
            "error: no EP-summary long CSVs found under "
            "data/ep_summaries/{verisk,risklink}/. "
            "Run `rollup ep-summary-to-csv` first.",
            file=sys.stderr,
        )
        return 2

    seeds_obj = load_all(cfg.seeds_dir)
    analyses = seeds_obj.analyses.collect()
    perils   = seeds_obj.perils.collect()

    df = derive_blending_weights(rl_csvs, vk_csvs, analyses, perils)
    output.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output)
    print(f"wrote {output}  ({df.height:,} rows)")
    print(df.head(20))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
