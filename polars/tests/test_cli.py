"""Unit tests for punch-list fixes in rollup.cli and rollup.config.

Covers:
  #1  f-string interpolation in _cmd_test_sql warning
  #2  bare `rollup` reaches the interactive plan flow
  #3  all_ylt_ok property + _cmd_run gates on it
  #4  push-to-sql exception path returns exit 2 with message
  #5  _cmd_run wraps pipeline exceptions cleanly
  #6  _load_local_config wraps exec_module errors cleanly
  #7  ep-summary-to-csv returns 2 when no xlsx found
  #8  redact_conn_str is public (no underscore)
  #11 docs subcommand is wired into the dispatch table
  #12 argcomplete autocomplete is invoked (parser builds without error)
"""
from __future__ import annotations

import io
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from rollup import config
from rollup.config import VendorName, redact_conn_str
from rollup.seeds import SEEDS


# ---------------------------------------------------------------------------
# Helpers shared across tests
# ---------------------------------------------------------------------------

def _make_config(tmp_path: Path, populate_seeds: bool = True) -> config.Config:
    """Minimal Config with real seeds copied in."""
    import shutil

    seeds_dir = tmp_path / "seeds"
    seeds_dir.mkdir()
    if populate_seeds:
        real_seeds = config.REPO_ROOT / "data" / "seeds"
        for spec in SEEDS:
            dest = seeds_dir / spec.filename
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(real_seeds / spec.filename, dest)
    return config.Config(
        seeds_dir=seeds_dir,
        output_dir=tmp_path / "out",
        vendors=(
            config.Vendor(VendorName.VERISK,   "AIR", 10_000,
                          tmp_path / "ylt" / VendorName.VERISK,
                          "air_ylt_*.parquet",
                          tmp_path / "ep" / VendorName.VERISK),
            config.Vendor(VendorName.RISKLINK, "RMS", 100_000,
                          tmp_path / "ylt" / VendorName.RISKLINK,
                          "risklink_ylt_*.parquet",
                          tmp_path / "ep" / VendorName.RISKLINK),
        ),
    )


# ---------------------------------------------------------------------------
# #8 redact_conn_str is public (no underscore)
# ---------------------------------------------------------------------------

def test_redact_conn_str_is_public():
    """redact_conn_str must be importable without the leading underscore."""
    assert callable(redact_conn_str)


def test_redact_conn_str_hides_credentials():
    result = redact_conn_str("mssql://user:secret@host:1433/db")
    assert "secret" not in result
    assert "...@host:1433/db" in result


def test_redact_conn_str_passthrough_odbc():
    odbc = "DRIVER={ODBC Driver};SERVER=localhost;Trusted_Connection=yes"
    assert redact_conn_str(odbc) == odbc


# ---------------------------------------------------------------------------
# #3 all_ylt_ok property
# ---------------------------------------------------------------------------

def test_all_ylt_ok_false_when_no_ylt_files(tmp_path):
    """Plan.all_ylt_ok is False when no YLT files exist for any vendor."""
    cfg = _make_config(tmp_path)
    plan = config.build_plan(cfg)
    # YLT dirs don't exist → all_ylt_ok must be False
    assert not plan.all_ylt_ok


def test_all_ylt_ok_false_when_only_one_vendor_has_files(tmp_path):
    """Plan.all_ylt_ok is False when only one of two vendors has YLT files."""
    import polars as pl
    cfg = _make_config(tmp_path)
    v = cfg.vendor(VendorName.VERISK)
    v.ylt_dir.mkdir(parents=True)
    # Write a minimal valid parquet that passes schema check
    from rollup.schemas import frames as F
    schema = F.RAW_VERISK_YLT
    dummy = pl.DataFrame({col: [] for col in schema.names()}, schema=schema)
    dummy.write_parquet(v.ylt_dir / "air_ylt_test.parquet")
    plan = config.build_plan(cfg)
    # Verisk has a file; RiskLink dir is absent → still False
    assert not plan.all_ylt_ok


def test_all_ylt_ok_true_when_both_vendors_have_files(tmp_path):
    """Plan.all_ylt_ok is True when both vendors have at least one valid YLT."""
    import polars as pl
    cfg = _make_config(tmp_path)
    from rollup.schemas import frames as F

    for vendor, glob, schema in [
        (cfg.vendor(VendorName.VERISK),   "air_ylt_test.parquet",      F.RAW_VERISK_YLT),
        (cfg.vendor(VendorName.RISKLINK), "risklink_ylt_test.parquet", F.RAW_RISKLINK_YLT),
    ]:
        vendor.ylt_dir.mkdir(parents=True, exist_ok=True)
        dummy = pl.DataFrame({col: [] for col in schema.names()}, schema=schema)
        dummy.write_parquet(vendor.ylt_dir / glob)

    plan = config.build_plan(cfg)
    assert plan.all_ylt_ok


# ---------------------------------------------------------------------------
# #2 bare `rollup` reaches interactive flow
# ---------------------------------------------------------------------------

def test_bare_rollup_prints_plan_and_aborts_without_tty(tmp_path, monkeypatch):
    """Invoking main() with no args starts the run flow, not argparse help."""
    import polars as pl
    from rollup.cli import main
    from rollup.schemas import frames as F

    cfg = _make_config(tmp_path)
    for vendor, glob, schema in [
        (cfg.vendor(VendorName.VERISK),   "air_ylt_test.parquet",      F.RAW_VERISK_YLT),
        (cfg.vendor(VendorName.RISKLINK), "risklink_ylt_test.parquet", F.RAW_RISKLINK_YLT),
    ]:
        vendor.ylt_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({col: [] for col in schema.names()}, schema=schema).write_parquet(vendor.ylt_dir / glob)
        vendor.ep_summary_dir.mkdir(parents=True, exist_ok=True)
        (vendor.ep_summary_dir / f"{vendor.name}.long.csv").write_text("analysis_id,peril,measure,return_period,loss\n")
    monkeypatch.setattr(config, "resolve", lambda: cfg)

    buf = io.StringIO()
    with patch("sys.stdout", buf):
        rc = main([])
    assert rc == 1
    output = buf.getvalue()
    assert "Pipeline plan" in output
    assert "usage:" not in output.lower()
    assert "refusing to run without --yes" in output


def test_rollup_dry_run_does_not_print_help(tmp_path, monkeypatch):
    """--dry-run bypasses the help-print gate and reaches the plan display."""
    from rollup.cli import main

    monkeypatch.setenv("ROLLUP_SEEDS_DIR", str(tmp_path / "seeds"))
    monkeypatch.setenv("ROLLUP_OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("ROLLUP_YLT_VERISK_DIR", str(tmp_path / "ylt_v"))
    monkeypatch.setenv("ROLLUP_YLT_RISKLINK_DIR", str(tmp_path / "ylt_r"))
    monkeypatch.setenv("ROLLUP_EP_VERISK_DIR", str(tmp_path / "ep_v"))
    monkeypatch.setenv("ROLLUP_EP_RISKLINK_DIR", str(tmp_path / "ep_r"))

    buf = io.StringIO()
    with patch("sys.stdout", buf):
        rc = main(["--dry-run"])
    # --dry-run should exit 0 (plan shown), not trigger help
    assert rc == 0
    output = buf.getvalue()
    # Plan output contains "Pipeline plan" or "[seeds]" — not the argparse usage line
    assert "Pipeline plan" in output or "[seeds]" in output


def test_audit_outputs_default_on_and_can_be_disabled():
    """Audit/debug parquets are now default-on for operator explainability."""
    from rollup.cli import _build_parser

    parser = _build_parser()

    assert parser.parse_args([]).dump_interim is True
    assert parser.parse_args(["--dump-interim"]).dump_interim is True
    assert parser.parse_args(["--no-audit"]).dump_interim is False


def test_run_time_blending_derivation_defaults_on_and_can_be_disabled():
    from rollup.cli import _build_parser

    parser = _build_parser()

    assert parser.parse_args([]).derive_blending is True
    assert parser.parse_args(["--derive-blending"]).derive_blending is True
    assert parser.parse_args(["--no-derive-blending"]).derive_blending is False
    assert parser.parse_args(["--use-blending-seed"]).derive_blending is False


# ---------------------------------------------------------------------------
# #5 _cmd_run wraps pipeline exceptions
# ---------------------------------------------------------------------------

def test_cmd_run_catches_pipeline_exception(tmp_path, monkeypatch, capsys):
    """When run() raises, _cmd_run returns 2 and prints a clean message."""
    import polars as pl
    from rollup.cli import main
    from rollup.schemas import frames as F

    seeds_dir = tmp_path / "seeds"
    seeds_dir.mkdir()
    import shutil
    real_seeds = config.REPO_ROOT / "data" / "seeds"
    for spec in SEEDS:
        dest = seeds_dir / spec.filename
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(real_seeds / spec.filename, dest)

    for vendor_name, glob, schema in [
        (VendorName.VERISK,   "air_ylt_test.parquet",      F.RAW_VERISK_YLT),
        (VendorName.RISKLINK, "risklink_ylt_test.parquet", F.RAW_RISKLINK_YLT),
    ]:
        ylt_dir = tmp_path / "ylt" / vendor_name
        ylt_dir.mkdir(parents=True)
        dummy = pl.DataFrame({col: [] for col in schema.names()}, schema=schema)
        dummy.write_parquet(ylt_dir / glob)

    monkeypatch.setenv("ROLLUP_SEEDS_DIR",         str(seeds_dir))
    monkeypatch.setenv("ROLLUP_OUTPUT_DIR",         str(tmp_path / "out"))
    monkeypatch.setenv("ROLLUP_YLT_VERISK_DIR",    str(tmp_path / "ylt" / VendorName.VERISK))
    monkeypatch.setenv("ROLLUP_YLT_RISKLINK_DIR",  str(tmp_path / "ylt" / VendorName.RISKLINK))
    monkeypatch.setenv("ROLLUP_EP_VERISK_DIR",     str(tmp_path / "ep_v"))
    monkeypatch.setenv("ROLLUP_EP_RISKLINK_DIR",   str(tmp_path / "ep_r"))

    with patch("rollup.pipeline.run", side_effect=RuntimeError("injected boom")):
        rc = main(["--yes", "--use-blending-seed"])

    assert rc == 2
    captured = capsys.readouterr()
    assert "pipeline failed" in captured.err
    assert "RuntimeError" in captured.err
    assert "injected boom" in captured.err
    assert "see docs/troubleshooting.md" in captured.err
    assert "http" not in captured.err.lower()


# ---------------------------------------------------------------------------
# #6 _load_local_config wraps exec_module errors
# ---------------------------------------------------------------------------

def test_load_local_config_raises_systemexit_on_syntax_error(tmp_path, monkeypatch):
    """A broken config.py causes SystemExit with a helpful message."""
    bad_config = tmp_path / "config.py"
    bad_config.write_text("this is not valid python !!! @@@")

    monkeypatch.setattr("rollup.config.REPO_ROOT", tmp_path)
    with pytest.raises(SystemExit) as exc_info:
        config._load_local_config()
    msg = str(exc_info.value)
    assert "config.py has a problem" in msg


# ---------------------------------------------------------------------------
# #7 ep-summary-to-csv returns 2 when no xlsx found
# ---------------------------------------------------------------------------

def test_ep_summary_to_csv_returns_2_when_no_files(tmp_path, monkeypatch, capsys):
    """_cmd_ep_summary_to_csv returns exit 2 when no xlsx files are found."""
    from rollup.cli import main

    monkeypatch.setenv("ROLLUP_SEEDS_DIR",         str(tmp_path / "seeds"))
    monkeypatch.setenv("ROLLUP_OUTPUT_DIR",         str(tmp_path / "out"))
    monkeypatch.setenv("ROLLUP_YLT_VERISK_DIR",    str(tmp_path / "ylt_v"))
    monkeypatch.setenv("ROLLUP_YLT_RISKLINK_DIR",  str(tmp_path / "ylt_r"))
    monkeypatch.setenv("ROLLUP_EP_VERISK_DIR",     str(tmp_path / "ep_v"))
    monkeypatch.setenv("ROLLUP_EP_RISKLINK_DIR",   str(tmp_path / "ep_r"))

    # ep dirs don't exist → no xlsx files
    rc = main(["ep-summary-to-csv"])
    assert rc == 2
    captured = capsys.readouterr()
    assert "error" in captured.err.lower()
    assert "xlsx" in captured.err.lower()


# ---------------------------------------------------------------------------
# #11 docs subcommand is registered in the dispatch table
# ---------------------------------------------------------------------------

def test_docs_subcommand_is_registered():
    """The 'docs' subcommand must appear in the parser's choices."""
    from rollup.cli import _build_parser

    parser = _build_parser()
    # Walk subparsers to find 'docs'
    subparsers_actions = [
        action for action in parser._actions
        if hasattr(action, "_name_parser_map")
    ]
    assert subparsers_actions, "no subparsers found"
    choices = list(subparsers_actions[0].choices.keys())
    assert "docs" in choices


def test_docs_subcommand_opens_browser_when_site_exists(tmp_path, monkeypatch, capsys):
    """_cmd_docs opens the browser and returns 0 when site/index.html exists."""
    from rollup.cli import _build_parser, _cmd_docs

    site_dir = tmp_path / "site"
    site_dir.mkdir()
    site_index = site_dir / "index.html"
    site_index.write_text("<html/>")

    opened_urls: list[str] = []
    monkeypatch.setattr("rollup.cli._docs_repo_root", lambda: tmp_path)
    monkeypatch.setattr("webbrowser.open", lambda url: opened_urls.append(url))

    rc = _cmd_docs(_build_parser().parse_args(["docs"]))

    assert rc == 0
    assert opened_urls == [site_index.as_uri()]
    assert f"opened {site_index}" in capsys.readouterr().out


def test_docs_subcommand_args_parsed_correctly():
    """docs --serve and docs --build flags parse without error."""
    from rollup.cli import _build_parser

    parser = _build_parser()
    args_serve = parser.parse_args(["docs", "--serve"])
    assert args_serve.cmd == "docs"
    assert args_serve.serve is True
    assert args_serve.build is False

    args_build = parser.parse_args(["docs", "--build"])
    assert args_build.cmd == "docs"
    assert args_build.build is True
    assert args_build.serve is False


# ---------------------------------------------------------------------------
# #12 argcomplete is wired into the parser (import-time check)
# ---------------------------------------------------------------------------

def test_argcomplete_is_installed():
    """argcomplete must be importable (added to pyproject.toml deps)."""
    import argcomplete  # noqa: F401
    assert True


def test_build_parser_completes_without_error():
    """_build_parser() must complete without raising (argcomplete wired in)."""
    from rollup.cli import _build_parser

    # argcomplete.autocomplete is called inside _build_parser; if it raises
    # (e.g. import error or API mismatch) the parser itself would blow up.
    parser = _build_parser()
    assert parser is not None


# ---------------------------------------------------------------------------
# #3 gate: _cmd_run aborts when all_ylt_ok is False
# ---------------------------------------------------------------------------

def test_cmd_run_aborts_when_ylt_missing(tmp_path, monkeypatch, capsys):
    """_cmd_run should return 2 and print an abort message when YLTs are absent."""
    import shutil
    from rollup.cli import main

    seeds_dir = tmp_path / "seeds"
    seeds_dir.mkdir()
    real_seeds = config.REPO_ROOT / "data" / "seeds"
    for spec in SEEDS:
        dest = seeds_dir / spec.filename
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(real_seeds / spec.filename, dest)

    monkeypatch.setenv("ROLLUP_SEEDS_DIR",        str(seeds_dir))
    monkeypatch.setenv("ROLLUP_OUTPUT_DIR",        str(tmp_path / "out"))
    monkeypatch.setenv("ROLLUP_YLT_VERISK_DIR",   str(tmp_path / "ylt_v"))   # doesn't exist
    monkeypatch.setenv("ROLLUP_YLT_RISKLINK_DIR", str(tmp_path / "ylt_r"))   # doesn't exist
    monkeypatch.setenv("ROLLUP_EP_VERISK_DIR",    str(tmp_path / "ep_v"))
    monkeypatch.setenv("ROLLUP_EP_RISKLINK_DIR",  str(tmp_path / "ep_r"))

    rc = main(["--yes"])
    assert rc == 2
    captured = capsys.readouterr()
    assert "fix the failing checks" in captured.err


# ---------------------------------------------------------------------------
# test-sql report rendering
# ---------------------------------------------------------------------------

def test_test_sql_missing_connection_message_is_single_error():
    from rollup.cli import _sql_conn_missing_message

    message = _sql_conn_missing_message()

    assert message.startswith("error:")
    assert message.endswith("\n")
    assert message.count("\n") == 1


def test_test_sql_report_success_is_single_stdout_block():
    from rollup.cli import _format_test_sql_report
    from rollup.io.sql_push import ConnectionTestResult

    result = ConnectionTestResult(
        ok=True,
        version="Microsoft SQL Server 2022\nextra details",
        database="rollup",
        schema="dbo",
        schema_exists=True,
        error=None,
    )

    code, stdout, stderr = _format_test_sql_report(
        redacted_conn_str="mssql://...@server/db",
        schema="dbo",
        result=result,
    )

    assert code == 0
    assert stderr == ""
    assert "SQL connection probe" in stdout
    assert "Target connection : mssql://...@server/db" in stdout
    assert "Status            : ok" in stdout
    assert "Server version   : Microsoft SQL Server 2022" in stdout
    assert "extra details" not in stdout
    assert "Schema 'dbo'         : exists" in stdout


def test_test_sql_report_missing_schema_returns_warning():
    from rollup.cli import _format_test_sql_report
    from rollup.io.sql_push import ConnectionTestResult

    result = ConnectionTestResult(
        ok=True,
        version="Microsoft SQL Server 2022",
        database="rollup",
        schema="marts",
        schema_exists=False,
        error=None,
    )

    code, stdout, stderr = _format_test_sql_report(
        redacted_conn_str="mssql://server/db",
        schema="marts",
        result=result,
    )

    assert code == 2
    assert "Schema 'marts'       : missing" in stdout
    assert "Warning: schema 'marts' not found" in stderr
    assert "push-to-sql --schema marts" in stderr
    assert "{args.schema}" not in stderr


def test_test_sql_report_connection_failure_goes_to_stderr():
    from rollup.cli import _format_test_sql_report
    from rollup.io.sql_push import ConnectionTestResult

    result = ConnectionTestResult(
        ok=False,
        version=None,
        database=None,
        schema=None,
        schema_exists=None,
        error="OperationalError: timeout",
    )

    code, stdout, stderr = _format_test_sql_report(
        redacted_conn_str="mssql://server/db",
        schema=None,
        result=result,
    )

    assert code == 2
    assert "Status            : failed" in stdout
    assert stderr == "Error: OperationalError: timeout\n"


# ---------------------------------------------------------------------------
# push-to-sql report rendering
# ---------------------------------------------------------------------------

def test_push_preview_lists_targets_and_destructive_note(tmp_path):
    from rollup.cli import _format_push_preview

    parquet = tmp_path / "HiscoAIR_202601_main.parquet"
    parquet.write_bytes(b"x" * 1_000_000)

    report = _format_push_preview(
        redacted_conn_str="mssql://...@server/db",
        schema="marts",
        output_dir=tmp_path,
        parquets=[parquet],
    )

    assert "SQL push preview" in report
    assert "Target connection : mssql://...@server/db" in report
    assert "Target schema     : marts" in report
    assert "dropped and replaced" in report
    assert "HiscoAIR_202601_main.parquet" in report
    assert "marts.HiscoAIR_202601_main" in report


def test_confirm_push_refuses_non_tty_without_yes():
    from rollup.cli import _confirm_push

    code, stdout, stderr = _confirm_push(False, stdin=io.StringIO(""))

    assert code == 2
    assert stdout == ""
    assert "refusing to push without --yes" in stderr


def test_confirm_push_abort_message_when_user_declines():
    from rollup.cli import _confirm_push

    class TtyInput(io.StringIO):
        def isatty(self):
            return True

    code, stdout, stderr = _confirm_push(
        False,
        stdin=TtyInput(""),
        input_func=lambda _: "n",
    )

    assert code == 1
    assert stdout == "aborted by user\n"
    assert stderr == ""


def test_push_error_report_includes_diagnostic_hint():
    from rollup.cli import _format_push_error

    report = _format_push_error("HiscoAIR.parquet", RuntimeError("boom"))

    assert "error pushing HiscoAIR.parquet: RuntimeError: boom" in report
    assert "rollup test-sql" in report
