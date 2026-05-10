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

    if args.cmd is None and not args.dry_run and not args.yes:
        parser.print_help()
        return 0

    handler = {
        "ep-summary-to-csv": _cmd_ep_summary_to_csv,
        "derive-blending":   _cmd_derive_blending,
        "test-sql":          _cmd_test_sql,
        "push-to-sql":       _cmd_push_to_sql,
        "docs":              _cmd_docs,
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
    parser.add_argument(
        "--min-loss", type=float, default=None, metavar="N",
        help="drop output rows whose loss < N. Default 1000 (production). "
             "Use --min-loss 0 to keep every event. Also settable via "
             "ROLLUP_MIN_LOSS env var or MIN_LOSS in config.py.",
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

    test_sql = sub.add_parser(
        "test-sql",
        help="Probe the SQL Server connection — connect, run @@VERSION, "
             "optionally check a schema exists. Read-only.",
    )
    test_sql.add_argument(
        "--schema", default=None, metavar="SCHEMA",
        help="If provided, also verify the schema exists on the server.",
    )

    push = sub.add_parser(
        "push-to-sql",
        help="Push the Hisco fanout parquets in data/output/ to SQL Server. "
             "Each table is dropped and recreated. Requires ROLLUP_MSSQL_CONN_STR "
             "(or MSSQL_CONN_STR in config.py).",
    )
    push.add_argument(
        "--schema", default=None, metavar="SCHEMA",
        help="SQL schema to push into (e.g. 'dbo', 'marts'). "
             "Default: server-side default (typically 'dbo').",
    )
    push.add_argument(
        "-y", "--yes", action="store_true", dest="push_yes",
        help="skip the y/N confirmation",
    )

    # NOTE: push-to-sql uses `--yes` / `-y` as `dest="push_yes"` (not "yes") to
    # avoid colliding with the top-level `-y/--yes` flag on the main parser.
    # This is intentional — renaming the push flag would break existing docs and
    # user scripts that already reference `push-to-sql --yes`.

    docs_p = sub.add_parser("docs", help="Open the pipeline documentation in your browser.")
    docs_p.add_argument(
        "--serve", action="store_true",
        help="Start zensical dev server with live reload (port 8000).",
    )
    docs_p.add_argument(
        "--build", action="store_true",
        help="Force rebuild of site/ before opening.",
    )

    import argcomplete
    argcomplete.autocomplete(parser)

    return parser


# --------------------------------------------------------------------------- #
# Subcommand handlers                                                         #
# --------------------------------------------------------------------------- #

def _cmd_run(args: argparse.Namespace) -> int:
    """Default flow: build the plan, prompt (or not), run the pipeline."""
    from dataclasses import replace

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
        # On stderr we use plain text — colour codes leak in CI logs.
        print(config.format_plan(plan), file=sys.stderr)
        print("aborting: fix the failing checks above, then re-run.", file=sys.stderr)
        return 2

    if not config.confirm(plan, assume_yes=args.yes):
        print("aborted by user")
        return 1

    try:
        run(cfg, dump_interim=args.dump_interim)
    except Exception as e:
        print(f"\npipeline failed: {type(e).__name__}: {e}", file=sys.stderr)
        print("see https://github.com/hamptonian/rollup-dbt/blob/master/docs/troubleshooting.md", file=sys.stderr)
        return 2
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
    if not written:
        print(
            "error: no xlsx EP summary files found under "
            "data/ep_summaries/{verisk,risklink}/. "
            "Drop your vendor xlsx exports there and retry.",
            file=sys.stderr,
        )
        return 2
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


def _cmd_test_sql(args: argparse.Namespace) -> int:
    """Read-only probe of the SQL Server connection.

    Connects using the configured connection string, runs `@@VERSION` +
    `DB_NAME()`, and optionally checks whether `--schema` exists. Prints a
    one-screen summary; never writes. Returns 0 on success, 2 on failure.
    """
    from rollup.config import redact_conn_str
    from rollup.io.sql_push import test_connection

    cfg = config.resolve()
    if not cfg.mssql_conn_str:
        print(
            "error: ROLLUP_MSSQL_CONN_STR (or MSSQL_CONN_STR in config.py) "
            "is not set. Cannot test SQL Server connection.",
            file=sys.stderr,
        )
        return 2

    redacted = redact_conn_str(cfg.mssql_conn_str)
    print()
    print(f"  Target connection : {redacted}")
    if args.schema:
        print(f"  Target schema     : {args.schema}")
    print()
    print("  Probing... ", end="", flush=True)

    result = test_connection(cfg.mssql_conn_str, schema=args.schema)

    if not result.ok:
        print("✘ failed")
        print()
        print(f"  Error: {result.error}", file=sys.stderr)
        print()
        return 2

    print("✓ ok")
    print()
    # @@VERSION can be multi-line — show only the headline.
    version_line = (result.version or "").splitlines()[0] if result.version else "(unknown)"
    print(f"  Server version  : {version_line}")
    print(f"  Database        : {result.database}")
    if args.schema:
        mark = "✓ exists" if result.schema_exists else "✘ does NOT exist on this server"
        print(f"  Schema {args.schema!r:14s}: {mark}")
        if not result.schema_exists:
            print()
            print(
                f"  Warning: schema {args.schema!r} not found on the server. "
                f"`push-to-sql --schema {args.schema}` will fail.",
                file=sys.stderr,
            )
            return 2
    print()
    return 0


def _cmd_push_to_sql(args: argparse.Namespace) -> int:
    """Push the Hisco fanout parquets in `data/output/` to SQL Server.

    Lists the parquets, prompts for confirmation, then drops + recreates each
    target table. The connection string comes from `ROLLUP_MSSQL_CONN_STR` (or
    `MSSQL_CONN_STR` in `config.py`); aborts with exit-2 if it isn't set.
    """
    from rollup.config import redact_conn_str
    from rollup.io.sql_push import list_pushable_parquets, make_engine, push_parquet_to_sql

    cfg = config.resolve()
    if not cfg.mssql_conn_str:
        print(
            "error: ROLLUP_MSSQL_CONN_STR (or MSSQL_CONN_STR in config.py) "
            "is not set. Cannot push to SQL Server.",
            file=sys.stderr,
        )
        return 2

    parquets = list_pushable_parquets(cfg.output_dir)
    if not parquets:
        print(
            f"error: no Hisco*.parquet files found under {cfg.output_dir}. "
            "Run `rollup --yes` first.",
            file=sys.stderr,
        )
        return 2

    redacted = redact_conn_str(cfg.mssql_conn_str)
    schema_label = args.schema or "<server-default>"

    print()
    print(f"  Target connection : {redacted}")
    print(f"  Target schema     : {schema_label}")
    print(f"  Output directory  : {cfg.output_dir}")
    print()
    print(f"  {len(parquets)} parquet file(s) will be pushed (existing tables of the same name will be DROPPED and replaced):")
    print()
    for p in parquets:
        size_mb = p.stat().st_size / 1e6
        table_name = p.stem
        fq_name = f"{schema_label}.{table_name}" if args.schema else table_name
        print(f"    {p.name:<40s}  ->  {fq_name:<48s}  ({size_mb:.1f} MB)")
    print()

    if not args.push_yes:
        if not sys.stdin.isatty():
            print(
                "error: refusing to push without --yes when stdin is not a TTY "
                "(piped / non-interactive). Re-run with `--yes` to confirm.",
                file=sys.stderr,
            )
            return 2
        try:
            reply = input("Proceed with push? [y/N]: ").strip().lower()
        except EOFError:
            reply = ""
        if reply not in {"y", "yes"}:
            print("aborted by user")
            return 1

    total_rows = 0
    engine = make_engine(cfg.mssql_conn_str)
    try:
        for p in parquets:
            try:
                n = push_parquet_to_sql(p, engine=engine, schema=args.schema)
            except Exception as e:
                print(f"\nerror pushing {p.name}: {type(e).__name__}: {e}", file=sys.stderr)
                print("hint: run `rollup test-sql` to diagnose the connection.", file=sys.stderr)
                return 2
            print(f"  pushed {p.name:<40s} ({n:,} rows)")
            total_rows += n
    finally:
        engine.dispose()

    print()
    print(f"  done: pushed {len(parquets)} table(s), {total_rows:,} rows total.")
    return 0


def _cmd_docs(args: argparse.Namespace) -> int:
    """Open the pipeline documentation in your browser."""
    import subprocess
    import webbrowser

    # repo root: cli.py is at polars/rollup/cli.py → ../../..
    repo_root = Path(__file__).resolve().parent.parent.parent
    site_index = repo_root / "site" / "index.html"

    if args.serve:
        webbrowser.open("http://localhost:8000")
        try:
            subprocess.run(["uv", "run", "zensical", "serve"], cwd=repo_root, check=True)
        except KeyboardInterrupt:
            pass
        return 0

    if args.build or not site_index.exists():
        print("building docs...", flush=True)
        try:
            subprocess.run(["uv", "run", "zensical", "build"], cwd=repo_root, check=True)
        except subprocess.CalledProcessError as e:
            print(f"error: docs build failed: {e}", file=sys.stderr)
            return 2

    if not site_index.exists():
        print(
            "error: site/ not built and build failed. Run `uv run zensical build` manually.",
            file=sys.stderr,
        )
        return 2

    webbrowser.open(site_index.as_uri())
    print(f"opened {site_index}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
