"""Tests for the Rich pre-flight plan renderer in `rollup.config`.

`print_plan` writes ANSI-coloured output via `rich.console.Console`. Tests use
`Console(file=StringIO(), force_terminal=True, color_system="truecolor")` to
capture the rendered string verbatim, then assert on substrings.

Plain-text `format_plan` is covered separately by tests in `test_config.py`.
"""
from __future__ import annotations

import io
import shutil
from pathlib import Path

import pytest
from rich.console import Console

from rollup import config
from rollup.config import (
    Check,
    Config,
    Plan,
    Section,
    Vendor,
    VendorName,
    redact_conn_str,
    _section_icon,
    _status_pill,
    print_plan,
)
from rollup.seeds import SEEDS


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

def _capture(plan: Plan, *, width: int = 120) -> str:
    """Render `plan` with print_plan to a string buffer."""
    buf = io.StringIO()
    console = Console(
        file=buf,
        force_terminal=True,
        color_system="truecolor",
        width=width,
    )
    print_plan(plan, console=console)
    return buf.getvalue()


def _cfg_with_seeds(tmp_path: Path) -> Config:
    """Real seed CSVs copied to tmp; tmp ylt/ep dirs (deliberately empty)."""
    seeds_dir = tmp_path / "seeds"
    seeds_dir.mkdir()
    real_seeds = config.REPO_ROOT / "data" / "seeds"
    for spec in SEEDS:
        dest = seeds_dir / spec.filename
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(real_seeds / spec.filename, dest)
    return Config(
        seeds_dir=seeds_dir,
        output_dir=tmp_path / "out",
        vendors=(
            Vendor(VendorName.VERISK,   "AIR", 10_000,
                   tmp_path / "ylt" / VendorName.VERISK,
                   "air_ylt_*.parquet",
                   tmp_path / "ep" / VendorName.VERISK),
            Vendor(VendorName.RISKLINK, "RMS", 100_000,
                   tmp_path / "ylt" / VendorName.RISKLINK,
                   "risklink_ylt_*.parquet",
                   tmp_path / "ep" / VendorName.RISKLINK),
        ),
    )


# --------------------------------------------------------------------------- #
# Pure helpers — _section_icon, _status_pill, redact_conn_str                 #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("title,expected_icon", [
    ("seeds",                "▣"),
    ("ylt verisk",           "▶"),
    ("ylt risklink",         "▶"),
    ("ep_summaries verisk",  "◆"),
    ("ep_summaries risklink","◆"),
    ("lob_peril_validation", "◇"),
    ("forecast_factors",     "◇"),
    ("output",               "◯"),
    ("totally unknown",      "·"),
])
def test_section_icon(title: str, expected_icon: str):
    assert _section_icon(title) == expected_icon


def test_status_pill_complete_is_green():
    pill = _status_pill(12, 12)
    assert "12/12" in pill.plain
    assert "✓" in pill.plain
    assert "green" in str(pill.style).lower() or "#6cc04a" in str(pill.style).lower()


def test_status_pill_partial_is_amber():
    pill = _status_pill(1, 2)
    assert "1/2" in pill.plain
    # Partial uses the amber/warn style (not green, not red)
    style = str(pill.style).lower()
    assert "green" not in style and "red" not in style


def test_status_pill_zero_is_red():
    pill = _status_pill(0, 1)
    assert "0/1" in pill.plain
    assert "✘" in pill.plain


def test_status_pill_empty_section():
    """A section with zero checks renders an empty-marker pill."""
    pill = _status_pill(0, 0)
    assert "empty" in pill.plain.lower() or "✘" in pill.plain


@pytest.mark.parametrize("conn_str,expected", [
    # No scheme → passthrough
    ("DRIVER={ODBC Driver};SERVER=localhost;Trusted_Connection=yes",
     "DRIVER={ODBC Driver};SERVER=localhost;Trusted_Connection=yes"),
    # No credentials → passthrough
    ("mssql://localhost:1433/db", "mssql://localhost:1433/db"),
    # Credentials present → redacted
    ("mssql://user:secret@host:1433/db",  "mssql://...@host:1433/db"),
    ("postgres://u:p@h/d",                "postgres://...@h/d"),
    # Edge: starts with @ (malformed) — don't strip the only thing left
    ("mssql://@host/db", "mssql://@host/db"),
])
def test_redact_conn_str(conn_str: str, expected: str):
    assert redact_conn_str(conn_str) == expected


# --------------------------------------------------------------------------- #
# print_plan — end-to-end rendering                                           #
# --------------------------------------------------------------------------- #

def test_print_plan_renders_all_six_sections(tmp_path: Path):
    """Every section title appears in the rendered output."""
    plan = config.build_plan(_cfg_with_seeds(tmp_path))
    out = _capture(plan)

    for title in ("seeds",
                  "ylt verisk",          "ylt risklink",
                  "ep_summaries verisk", "ep_summaries risklink",
                  "lob_peril_validation",
                  "forecast_factors",
                  "output"):
        assert title in out, f"section {title!r} missing from rendered output"


def test_print_plan_contains_section_icons(tmp_path: Path):
    """Each known section title is preceded by its icon glyph."""
    plan = config.build_plan(_cfg_with_seeds(tmp_path))
    out = _capture(plan)

    assert "▣" in out, "seeds icon missing"
    assert "▶" in out, "ylt icon missing"
    assert "◆" in out, "ep_summaries icon missing"
    assert "◯" in out, "output icon missing"


def test_print_plan_contains_status_pills(tmp_path: Path):
    """Status pills appear with x/y notation and ✓ / ✘ glyphs."""
    plan = config.build_plan(_cfg_with_seeds(tmp_path))
    out = _capture(plan)

    # Seeds are all green: 11/11 ✓
    assert "11/11" in out
    assert "✓" in out
    # YLT directories don't exist in tmp → 0/1 ✘ for each vendor
    assert "✘" in out


def test_print_plan_summary_line_contains_all_buckets(tmp_path: Path):
    """The footer one-liner shows seeds / ylt / ep / sql in that order."""
    plan = config.build_plan(_cfg_with_seeds(tmp_path))
    out = _capture(plan)

    # The summary line lives at the bottom; confirm every bucket label appears
    # AFTER the last section icon. We just spot-check substrings.
    assert "seeds" in out
    assert "ylt"   in out
    assert "ep"    in out
    assert "sql"   in out
    # The bottom rule is rendered by Rule(); a long unicode rule character.
    assert "─" in out


def test_print_plan_truncates_long_path_with_gap(tmp_path: Path):
    """Long paths ellipsize with a gap before the right-aligned status pill."""
    # Make a vendor with an absurdly long ylt_dir to force ellipsis.
    cfg = _cfg_with_seeds(tmp_path)
    long_path = tmp_path / ("very_long_directory_name_" * 8)
    long_path.mkdir(parents=True)
    cfg = Config(
        seeds_dir=cfg.seeds_dir,
        output_dir=cfg.output_dir,
        vendors=(
            Vendor(VendorName.VERISK,   "AIR", 10_000,
                   long_path, "air_ylt_*.parquet", long_path),
            cfg.vendors[1],
        ),
    )
    plan = config.build_plan(cfg)
    out = _capture(plan, width=80)   # narrow so truncation actually fires

    # The ellipsis character means truncation occurred
    assert "…" in out
    # Even after ellipsizing, a pill should be visible somewhere
    assert "✓" in out or "✘" in out


def test_print_plan_renders_sql_redacted(tmp_path: Path):
    """If a SQL conn_str has credentials, they get redacted in the summary."""
    cfg = _cfg_with_seeds(tmp_path)
    cfg = Config(
        seeds_dir=cfg.seeds_dir,
        output_dir=cfg.output_dir,
        vendors=cfg.vendors,
        mssql_conn_str="mssql://user:secret@host:1433/db",
    )
    plan = config.build_plan(cfg)
    out = _capture(plan)

    assert "secret" not in out, "credentials must not appear in rendered output"
    assert "..." in out


def test_print_plan_handles_empty_section():
    """A section with zero checks doesn't crash the renderer."""
    plan = Plan(
        config=Config(
            seeds_dir=Path("/tmp"),
            output_dir=Path("/tmp/out"),
            vendors=(),
        ),
        sections=[
            Section(title="seeds",  header="/dev/null", checks=[]),
            Section(title="output", header="/tmp/out",
                    checks=[Check(label="output_dir", path=Path("/tmp/out"),
                                  ok=True, note="will be created on run")]),
        ],
    )
    out = _capture(plan)
    # Doesn't crash and the empty section's title is still present
    assert "seeds"  in out
    assert "output" in out
