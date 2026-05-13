"""Interactive/default run orchestration.

The CLI module should parse arguments and dispatch. This module owns the run
flow: resolve configuration, build/render the plan, collect optional run inputs,
confirm with the operator, and invoke the pipeline.
"""

from __future__ import annotations

import sys
from dataclasses import replace
from typing import Protocol

from rollup import config
from rollup.run_inputs import derive_blending_for_run


_YES = {"y", "yes"}


class RunArgs(Protocol):
    """Subset of argparse.Namespace needed by the run wizard."""

    min_loss: float | None
    dry_run: bool
    yes: bool
    dump_interim: bool
    derive_blending: bool


def run_wizard(args: RunArgs) -> int:
    """Run the default rollup command flow."""
    from rollup.pipeline import run

    cfg = config.resolve()
    if args.min_loss is not None:
        cfg = replace(cfg, min_loss=args.min_loss)
    plan = config.build_plan(cfg, require_ep_summaries=args.derive_blending)

    if args.dry_run:
        if sys.stdout.isatty():
            config.print_plan(plan)
        else:
            print(config.format_plan(plan))
        return 0

    if not plan.all_seeds_ok or not plan.all_ylt_ok or (args.derive_blending and not plan.all_ep_ok):
        print(config.format_plan(plan), file=sys.stderr)
        print("aborting: fix the failing checks above, then re-run.", file=sys.stderr)
        return 2

    if args.yes:
        if not config.confirm(plan, assume_yes=True, stream=sys.stdout):
            print("aborted by user")
            return 1
    elif sys.stdin.isatty():
        if not _interactive_review(cfg, plan, args):
            print("aborted by user")
            return 1
    elif not config.confirm(plan, assume_yes=False, stream=sys.stdout):
        print("aborted by user")
        return 1

    blending_weights = None
    if args.derive_blending:
        blending = derive_blending_for_run(cfg)
        blending_weights = blending.weights
        print(blending.message)
    else:
        print("blending: using blending_weights.csv (--no-derive-blending)")

    try:
        run(cfg, dump_interim=args.dump_interim, blending_weights=blending_weights)
    except Exception as e:
        print(f"\npipeline failed: {type(e).__name__}: {e}", file=sys.stderr)
        print("see docs/troubleshooting.md", file=sys.stderr)
        return 2

    if not args.yes and sys.stdin.isatty() and cfg.mssql_conn_str:
        _maybe_push_sql()
    return 0


def _interactive_review(cfg: config.Config, plan: config.Plan, args: RunArgs) -> bool:
    """Operator wizard for TTY runs."""
    config.print_plan(plan)
    print("Run settings")
    print(f"  seeds       : {cfg.seeds_dir}")
    print(f"  output      : {cfg.output_dir}")
    for vendor in cfg.vendors:
        print(f"  {vendor.name} YLT : {vendor.ylt_dir} ({vendor.ylt_glob})")
        print(f"  {vendor.name} EP  : {vendor.ep_summary_dir} (*.long.csv)")
    print(f"  min loss    : {cfg.min_loss:g}")
    print(f"  audit       : {'on' if args.dump_interim else 'off'}")
    print(f"  blending    : {'derive from EP summaries' if args.derive_blending else 'reviewed blending_weights.csv'}")
    print(f"  SQL push    : {'available after run' if cfg.mssql_conn_str else 'not configured'}")
    return _ask_yes("Proceed with rollup run? [y/N]: ")


def _ask_yes(prompt: str) -> bool:
    try:
        return input(prompt).strip().lower() in _YES
    except EOFError:
        return False


def _maybe_push_sql() -> None:
    if not _ask_yes("Push output parquets to SQL now? [y/N]: "):
        return
    from argparse import Namespace
    from rollup.cli import _cmd_push_to_sql

    schema = input("SQL schema [server default]: ").strip() or None
    code = _cmd_push_to_sql(Namespace(schema=schema, push_yes=True))
    if code:
        print(f"SQL push failed with exit code {code}; run `rollup push-to-sql` after fixing it.", file=sys.stderr)
