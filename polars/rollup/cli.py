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
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

from rollup import config

if TYPE_CHECKING:
    from rollup.io.sql_push import ConnectionTestResult


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    config.setup_logging(args.log_level)

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

    # Default-flow flags (no subcommand) — bare `rollup` runs the interactive
    # wizard, `rollup --yes` runs non-interactively, and `rollup --dry-run`
    # prints the plan. Kept on the top-level parser so users don't need a
    # separate `run` subcommand.
    parser.add_argument(
        "-y", "--yes", action="store_true",
        help="skip the interactive y/N confirmation",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="print the plan and exit without running the pipeline",
    )
    audit_group = parser.add_mutually_exclusive_group()
    audit_group.add_argument(
        "-d", "--dump-interim", dest="dump_interim", action="store_true", default=True,
        help="write audit_wide.parquet + audit_long.parquet to <output_dir>/debug/ "
             "for read-across verification (default)",
    )
    audit_group.add_argument(
        "--no-audit", dest="dump_interim", action="store_false",
        help="skip debug audit_wide.parquet and audit_long.parquet outputs",
    )
    parser.add_argument(
        "--min-loss", type=float, default=None, metavar="N",
        help="drop output rows whose loss < N. Default 1000 (production). "
             "Use --min-loss 0 to keep every event. Also settable via "
             "ROLLUP_MIN_LOSS env var or MIN_LOSS in config.py.",
    )
    blend_group = parser.add_mutually_exclusive_group()
    blend_group.add_argument(
        "--derive-blending", dest="derive_blending", action="store_true", default=True,
        help="derive blending weights in-memory for this run when all vendor EP-summary long CSVs are present (default)",
    )
    blend_group.add_argument(
        "--no-derive-blending", dest="derive_blending", action="store_false",
        help="always use data/seeds/vor/blending_weights.csv for this run",
    )
    blend_group.add_argument(
        "--use-blending-seed", dest="derive_blending", action="store_false",
        help="explicitly use reviewed data/seeds/vor/blending_weights.csv instead of run-time EP derivation",
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
    from rollup.wizard import run_wizard

    return run_wizard(args)


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
        sys.stderr.write(_sql_conn_missing_message())
        return 2

    redacted = redact_conn_str(cfg.mssql_conn_str)
    result = test_connection(cfg.mssql_conn_str, schema=args.schema)
    exit_code, stdout, stderr = _format_test_sql_report(
        redacted_conn_str=redacted,
        schema=args.schema,
        result=result,
    )
    sys.stdout.write(stdout)
    sys.stderr.write(stderr)
    return exit_code


def _sql_conn_missing_message() -> str:
    return (
        "error: ROLLUP_MSSQL_CONN_STR (or MSSQL_CONN_STR in config.py) "
        "is not set. Cannot test SQL Server connection.\n"
    )


def _format_test_sql_report(
    *,
    redacted_conn_str: str,
    schema: str | None,
    result: "ConnectionTestResult",
) -> tuple[int, str, str]:
    """Render `rollup test-sql` output in one place.

    Returns `(exit_code, stdout, stderr)`. Keeping formatting pure makes the
    command handler a simple sequence of resolve → probe → render.
    """
    stdout_lines = [
        "",
        "SQL connection probe",
        f"  Target connection : {redacted_conn_str}",
    ]
    if schema:
        stdout_lines.append(f"  Target schema     : {schema}")

    if not result.ok:
        stdout_lines.append("  Status            : failed")
        return 2, _lines(stdout_lines), _lines([f"Error: {result.error}"])

    stdout_lines.append("  Status            : ok")

    # @@VERSION can be multi-line — show only the headline.
    version_line = (result.version or "").splitlines()[0] if result.version else "(unknown)"
    stdout_lines.extend([
        f"  Server version   : {version_line}",
        f"  Database         : {result.database}",
    ])
    if schema:
        schema_status = "exists" if result.schema_exists else "missing"
        stdout_lines.append(f"  Schema {schema!r:14s}: {schema_status}")
        if not result.schema_exists:
            return 2, _lines(stdout_lines), _lines([
                f"Warning: schema {schema!r} not found on the server. "
                f"`push-to-sql --schema {schema}` will fail."
            ])
    return 0, _lines(stdout_lines), ""


def _lines(lines: list[str]) -> str:
    return "\n".join(lines) + "\n"


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
        sys.stderr.write(_push_conn_missing_message())
        return 2

    parquets = list_pushable_parquets(cfg.output_dir)
    if not parquets:
        sys.stderr.write(_push_no_parquets_message(cfg.output_dir))
        return 2

    redacted = redact_conn_str(cfg.mssql_conn_str)
    sys.stdout.write(_format_push_preview(
        redacted_conn_str=redacted,
        schema=args.schema,
        output_dir=cfg.output_dir,
        parquets=parquets,
    ))

    confirm_code, confirm_stdout, confirm_stderr = _confirm_push(args.push_yes)
    sys.stdout.write(confirm_stdout)
    sys.stderr.write(confirm_stderr)
    if confirm_code is not None:
        return confirm_code

    total_rows = 0
    engine = make_engine(cfg.mssql_conn_str)
    try:
        for p in parquets:
            try:
                n = push_parquet_to_sql(p, engine=engine, schema=args.schema)
            except Exception as e:
                sys.stderr.write(_format_push_error(p.name, e))
                return 2
            sys.stdout.write(_lines([f"  pushed {p.name:<40s} ({n:,} rows)"]))
            total_rows += n
    finally:
        engine.dispose()

    sys.stdout.write(_format_push_done(table_count=len(parquets), row_count=total_rows))
    return 0


def _push_conn_missing_message() -> str:
    return (
        "error: ROLLUP_MSSQL_CONN_STR (or MSSQL_CONN_STR in config.py) "
        "is not set. Cannot push to SQL Server.\n"
    )


def _push_no_parquets_message(output_dir: Path) -> str:
    return (
        f"error: no Hisco*.parquet files found under {output_dir}. "
        "Run `rollup --yes` first.\n"
    )


def _format_push_preview(
    *,
    redacted_conn_str: str,
    schema: str | None,
    output_dir: Path,
    parquets: Sequence[Path],
) -> str:
    schema_label = schema or "<server-default>"
    lines = [
        "",
        "SQL push preview",
        f"  Target connection : {redacted_conn_str}",
        f"  Target schema     : {schema_label}",
        f"  Output directory  : {output_dir}",
        "",
        f"  {len(parquets)} parquet file(s) will be pushed; existing tables with matching names will be dropped and replaced:",
        "",
    ]
    for path in parquets:
        size_mb = path.stat().st_size / 1e6
        target_name = f"{schema_label}.{path.stem}" if schema else path.stem
        lines.append(f"    {path.name:<40s}  ->  {target_name:<48s}  ({size_mb:.1f} MB)")
    lines.append("")
    return _lines(lines)


def _confirm_push(
    push_yes: bool,
    *,
    stdin=None,
    input_func: Callable[[str], str] = input,
) -> tuple[int | None, str, str]:
    """Return `(exit_code, stdout, stderr)` for push confirmation.

    `exit_code=None` means the caller may continue with the push.
    """
    if push_yes:
        return None, "", ""
    stdin = stdin or sys.stdin
    if not stdin.isatty():
        return 2, "", _lines([
            "error: refusing to push without --yes when stdin is not a TTY "
            "(piped / non-interactive). Re-run with `--yes` to confirm."
        ])
    try:
        reply = input_func("Proceed with push? [y/N]: ").strip().lower()
    except EOFError:
        reply = ""
    if reply not in {"y", "yes"}:
        return 1, _lines(["aborted by user"]), ""
    return None, "", ""


def _format_push_error(parquet_name: str, error: Exception) -> str:
    return _lines([
        f"error pushing {parquet_name}: {type(error).__name__}: {error}",
        "hint: run `rollup test-sql` to diagnose the connection.",
    ])


def _format_push_done(*, table_count: int, row_count: int) -> str:
    return _lines(["", f"  done: pushed {table_count} table(s), {row_count:,} rows total."])


def _cmd_docs(args: argparse.Namespace) -> int:
    """Open the pipeline documentation in your browser."""
    import subprocess
    import webbrowser

    repo_root = _docs_repo_root()
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


def _docs_repo_root() -> Path:
    """Repository root used by the docs subcommand."""
    # cli.py is at polars/rollup/cli.py → ../../..
    return Path(__file__).resolve().parent.parent.parent


if __name__ == "__main__":
    raise SystemExit(main())
