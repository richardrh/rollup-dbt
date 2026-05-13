"""Interactive/default run orchestration.

The CLI module should parse arguments and dispatch. This module owns the run
flow: resolve configuration, build/render the plan, collect optional run inputs,
confirm with the operator, and invoke the pipeline.
"""

from __future__ import annotations

import sys
from dataclasses import replace
from typing import TextIO
from typing import Protocol

from rich.console import Console

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
        _render_plan(plan, stream=sys.stdout)
        return 2 if plan.has_lob_peril_conflict else 0

    if (
        not plan.all_seeds_ok
        or not plan.all_ylt_ok
        or (args.derive_blending and not plan.all_ep_ok)
        or not plan.all_lob_peril_ok
    ):
        _render_plan(plan, stream=sys.stderr)
        print("aborting: fix the failing checks above, then re-run.", file=sys.stderr)
        return 2

    if args.yes:
        if not config.confirm(plan, assume_yes=True, stream=sys.stdout):
            print("aborted by user")
            return 1
    elif sys.stdin.isatty():
        reviewed_cfg = _interactive_review(cfg, plan, args)
        if reviewed_cfg is None:
            print("aborted by user")
            return 1
        cfg = reviewed_cfg
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


def _interactive_review(cfg: config.Config, plan: config.Plan, args: RunArgs) -> config.Config | None:
    """Operator wizard for TTY runs."""
    _render_plan(plan, stream=sys.stdout)
    print("Input paths")
    print(f"  seeds       : {cfg.seeds_dir}")
    print(f"  output      : {cfg.output_dir}")
    for vendor in cfg.vendors:
        print(f"  {vendor.name} YLT : {vendor.ylt_dir} ({vendor.ylt_glob})")
        print(f"  {vendor.name} EP  : {vendor.ep_summary_dir} (*.long.csv)")
    if not _ask_yes("Use these input paths? [Y/n]: ", default=True):
        return None

    forecast = _forecast_summary(plan)
    print("Forecast factors")
    print(f"  {forecast}")
    if not _ask_yes("Continue with these forecast factors? [Y/n]: ", default=True):
        return None

    print("Blending")
    if args.derive_blending:
        if not _ask_yes("Derive blending from EP-summary long CSVs? [Y/n]: ", default=True):
            args.derive_blending = False
            print("  using reviewed blending_weights.csv for this run")
    else:
        print("  using reviewed blending_weights.csv for this run")

    cfg = replace(cfg, min_loss=_prompt_min_loss(cfg.min_loss))
    if not _ask_yes(f"Write debug audit outputs? [{'Y/n' if args.dump_interim else 'y/N'}]: ", default=args.dump_interim):
        args.dump_interim = False
    else:
        args.dump_interim = True

    print(f"SQL push after run: {'available' if cfg.mssql_conn_str else 'not configured'}")
    return cfg if _ask_yes("Proceed with full rollup run? [y/N]: ") else None


def _ask_yes(prompt: str, *, default: bool = False) -> bool:
    try:
        reply = input(prompt).strip().lower()
    except EOFError:
        return False
    if not reply:
        return default
    return reply in _YES


def _prompt_min_loss(current: float) -> float:
    try:
        reply = input(f"Minimum loss threshold [{current:g}]: ").strip()
    except EOFError:
        return current
    if not reply:
        return current
    try:
        return float(reply)
    except ValueError:
        print(f"invalid min loss {reply!r}; keeping {current:g}")
        return current


def _forecast_summary(plan: config.Plan) -> str:
    section = next((s for s in plan.sections if s.title == "forecast_factors"), None)
    if section is None:
        return "forecast_factors section missing"
    return "; ".join(f"{c.label}: {c.note}" for c in section.checks if c.note) or "no forecast details"


def _render_plan(plan: config.Plan, *, stream: TextIO) -> None:
    """Render the plan with Rich for terminals and plain text for pipes."""
    if getattr(stream, "isatty", lambda: False)():
        config.print_plan(plan, console=Console(file=stream))
    else:
        print(config.format_plan(plan), file=stream)


def _maybe_push_sql() -> None:
    if not _ask_yes("Push output parquets to SQL now? [y/N]: "):
        return
    from argparse import Namespace
    from rollup.cli import _cmd_push_to_sql

    schema = input("SQL schema [server default]: ").strip() or None
    code = _cmd_push_to_sql(Namespace(schema=schema, push_yes=True))
    if code:
        print(f"SQL push failed with exit code {code}; run `rollup push-to-sql` after fixing it.", file=sys.stderr)
