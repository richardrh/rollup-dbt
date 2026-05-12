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
    plan = config.build_plan(cfg)

    if args.dry_run:
        if sys.stdout.isatty():
            config.print_plan(plan)
        else:
            print(config.format_plan(plan))
        return 0

    if not plan.all_seeds_ok or not plan.all_ylt_ok:
        print(config.format_plan(plan), file=sys.stderr)
        print("aborting: fix the failing checks above, then re-run.", file=sys.stderr)
        return 2

    if not config.confirm(plan, assume_yes=args.yes, stream=sys.stdout):
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
    return 0
