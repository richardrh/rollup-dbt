"""Pipeline configuration: vendors, paths, and the pre-run plan reporter.

One place that answers four questions:

  1. Who are the vendors, and how many simulation years does each have?
     → `Vendor`, `_verisk()`, `_risklink()`.
  2. Where does data live on disk?
     → `Config.seeds_dir`, `output_dir`, and each vendor's
     `ylt_dir` + `ylt_glob` + `ep_summary_dir`.
  3. Are all the required inputs present and schema-valid?
     → `build_plan(config)` returns a `Plan` with per-file status.
  4. Does the user want to run?
     → `confirm(plan, assume_yes=bool)` prints + prompts.

Default layout:

    <repo>/
    ├── polars/
    │   └── rollup/              ← this package (source code only)
    └── data/                    ← all user-owned input/output
        ├── seeds/               ← reference CSVs (git-tracked — refreshed periodically)
        ├── ylt/
        │   ├── verisk/*.parquet (≈ 10 000 simulation years)
        │   └── risklink/*.parquet (≈ 100 000 simulation years)
        ├── ep_summaries/
        │   ├── verisk/        ← reference AIR comparators (xlsx or csv)
        │   └── risklink/      ← reference RMS comparators (xlsx or csv)
        └── output/              ← Hisco parquets written here

Override any path with the corresponding `ROLLUP_*` env var, or set them
in `config.py` at the repo root (gitignored — never committed).
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

import importlib.util

import polars as pl
from rich.console import Console, Group
from rich.padding import Padding
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from rollup.schemas import frames as F
from rollup.seeds import REQUIRED_SEEDS, SeedSpec, discover as discover_seeds
from rollup.validate import ColumnDiff, column_diff


# Hiscox-y palette — used by the Rich renderer below.
# `format_plan` (plain text for tests / pipes) ignores these.
_BRAND      = "bold #B22234"      # the one accent — title only
_RULE       = "#5A1A28"           # darker red for rules
_OK         = "bold #6CC04A"      # green checkmark
_WARN       = "bold #E5A53B"      # amber for partials
_FAIL       = "bold #D14B4B"      # red ✘
_INK        = "bright_white"      # primary text
_BODY       = "white"             # secondary text
_DIM        = "grey50"            # tertiary / paths
_LABEL      = "bold #E5B36B"      # section names
_NUM        = "#E5B36B"           # numeric counts
_GLYPH_OK   = "✓"
_GLYPH_FAIL = "✘"
_GLYPH_WARN = "•"

# Section icon — visual key on the left of every section header.
_SECTION_ICONS: dict[str, str] = {
    "seeds":        "▣",
    "ylt":          "▶",
    "ep_summaries": "◆",
    "output":       "◯",
}


# --------------------------------------------------------------------------- #
# Repo paths                                                                  #
# --------------------------------------------------------------------------- #

POLARS_ROOT = Path(__file__).resolve().parent.parent    # .../polars
REPO_ROOT   = POLARS_ROOT.parent                        # .../rollup-dbt


# --------------------------------------------------------------------------- #
# Domain constants                                                            #
# --------------------------------------------------------------------------- #

class VendorName(StrEnum):
    """Closed set of vendor identifiers — the string that appears in the
    `vendor` column of the NormalizedYlt and in `base_model` after uplift.

    StrEnum members ARE strings, so `pl.col(Y.VENDOR) == VendorName.VERISK`
    and `pl.lit(VendorName.VERISK)` both work as drop-in replacements for
    raw string literals. The short forms `vk` / `rl` are used as code-level
    shorthand in working column names (`VK_PROPORTION`, `_vk_aal`) and
    variable names (`vk_norm`, `n_sim_vk`) — never as data values.
    """
    VERISK   = "verisk"
    RISKLINK = "risklink"


class CurrencyCode(StrEnum):
    """Closed set of currency codes that the pipeline derives in code
    (`attach_currency`). The seed `fx_rates.csv` may contain other codes —
    this enum only covers what the derivation logic emits.

    Add a member here AND a row to `fx_rates.csv` (with target=GBP) when
    extending the currency-derivation rule in `attach_currency`.
    """
    GBP = "GBP"
    EUR = "EUR"


# Peril families whose `base_model` is forced to RiskLink regardless of the
# YLT row's vendor — Verisk does not model flood, so the blended AAL must
# use the RiskLink AAL as denominator. Used in `attach_uplift` against
# the row's `peril_family` (joined from perils.csv).
#
# Single semantic value: any peril whose family is "FL" gets risklink as
# base. No region prefix. New flood region in perils.csv → no code change.
FLOOD_FAMILY: str = "FL"


class EnvVar(StrEnum):
    """Every `ROLLUP_*` env var the pipeline reads — defined in one place.

    StrEnum members ARE strings, so `os.getenv(EnvVar.LOG)` and
    `monkeypatch.setenv(EnvVar.SEEDS_DIR, value)` both work. Use these
    everywhere — never type the raw `"ROLLUP_*"` string in code or tests.
    """
    LOG               = "ROLLUP_LOG"
    DATA_DIR          = "ROLLUP_DATA_DIR"
    SEEDS_DIR         = "ROLLUP_SEEDS_DIR"
    OUTPUT_DIR        = "ROLLUP_OUTPUT_DIR"
    YLT_VERISK_DIR    = "ROLLUP_YLT_VERISK_DIR"
    YLT_VERISK_GLOB   = "ROLLUP_YLT_VERISK_GLOB"
    YLT_RISKLINK_DIR  = "ROLLUP_YLT_RISKLINK_DIR"
    YLT_RISKLINK_GLOB = "ROLLUP_YLT_RISKLINK_GLOB"
    EP_VERISK_DIR     = "ROLLUP_EP_VERISK_DIR"
    EP_RISKLINK_DIR   = "ROLLUP_EP_RISKLINK_DIR"
    MSSQL_CONN_STR    = "ROLLUP_MSSQL_CONN_STR"
    MIN_LOSS          = "ROLLUP_MIN_LOSS"


# --------------------------------------------------------------------------- #
# Logging                                                                     #
# --------------------------------------------------------------------------- #

def setup_logging(level: str | None = None) -> None:
    """Initialise the `rollup` logger. Silent by default (WARNING)."""
    resolved = level or os.getenv(EnvVar.LOG, "WARNING")
    logging.basicConfig(
        level=resolved.upper(),
        format="%(asctime)s  %(levelname)-5s  %(name)-22s  %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )


# --------------------------------------------------------------------------- #
# Fan-out flavours                                                            #
# --------------------------------------------------------------------------- #

class Flavor(StrEnum):
    """Two Hisco output flavours.

    Every factor (uplift, cap, FX, forecast, euws, fa_gross) is just a
    multiplier in the chain. The `MAIN` flavour is the loss with the full
    chain applied. `DIALSUP` is a sensitivity computed as a ratio applied
    to the RAW loss.

      * MAIN    — the normal deliverable: capped, local-ccy, forecast-adjusted,
                  euws-adjusted, fine-art correction applied. fa_gross is
                  just one of the factors in the chain — not a first-class
                  concept, the same way forecast and FX are just factors.
      * DIALSUP — sensitivity scenario: the composite (forecast × euws ×
                  fa_gross) factor applied directly to the raw loss,
                  bypassing uplift / cap / FX.
    """
    MAIN    = "main"
    DIALSUP = "dialsup"


# --------------------------------------------------------------------------- #
# Vendors                                                                     #
# --------------------------------------------------------------------------- #

_DEFAULT_FLAVORS: tuple[Flavor, ...] = (Flavor.MAIN, Flavor.DIALSUP)


@dataclass(frozen=True)
class Vendor:
    """Everything that varies by vendor, in one place.

    `name` is the `VendorName` enum member that appears in the YLT `vendor`
    column throughout the pipeline. `hisco_label` only appears in output
    filenames (`HiscoAIR_*` / `HiscoRMS_*`) — these are the external contract
    and must stay as-is.

    `flavors` declares which Hisco flavours this vendor produces. Per
    vendor × forecast_date (from the `forecast_factors` seed) × flavor
    we emit one Hisco parquet.
    """
    name:            VendorName
    hisco_label:     str              # "AIR" | "RMS" — output filename prefix
    n_simulations:   int              # 10_000 | 100_000
    ylt_dir:         Path
    ylt_glob:        str
    ep_summary_dir:  Path
    ep_summary_glob: str              = "*"   # reference comparators (xlsx or csv); not a pipeline input
    flavors:         tuple[Flavor, ...] = _DEFAULT_FLAVORS


def _env_path(var: EnvVar, default: Path) -> Path:
    raw = os.getenv(var)
    return Path(raw).expanduser().resolve() if raw else default


def _verisk(data_root: Path) -> Vendor:
    return Vendor(
        name=VendorName.VERISK,
        hisco_label="AIR",
        n_simulations=10_000,
        ylt_dir=_env_path(EnvVar.YLT_VERISK_DIR, data_root / "ylt" / VendorName.VERISK),
        ylt_glob=os.getenv(EnvVar.YLT_VERISK_GLOB, "air_ylt_*.parquet"),
        ep_summary_dir=_env_path(EnvVar.EP_VERISK_DIR, data_root / "ep_summaries" / VendorName.VERISK),
    )


def _risklink(data_root: Path) -> Vendor:
    return Vendor(
        name=VendorName.RISKLINK,
        hisco_label="RMS",
        n_simulations=100_000,
        ylt_dir=_env_path(EnvVar.YLT_RISKLINK_DIR, data_root / "ylt" / VendorName.RISKLINK),
        ylt_glob=os.getenv(EnvVar.YLT_RISKLINK_GLOB, "risklink_ylt_*.parquet"),
        ep_summary_dir=_env_path(EnvVar.EP_RISKLINK_DIR, data_root / "ep_summaries" / VendorName.RISKLINK),
    )


# --------------------------------------------------------------------------- #
# Top-level config                                                            #
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Config:
    seeds_dir:      Path
    output_dir:     Path
    vendors:        tuple[Vendor, ...]
    mssql_conn_str: str | None = None   # None → parquet-only run
    # Final-stage threshold: rows whose loss column is below this value are
    # dropped from every output. Default 1000.0 — small-loss events bloat
    # the parquets without contributing meaningfully to EP curves. Override
    # to 0.0 (CLI `--min-loss 0`, env `ROLLUP_MIN_LOSS=0`, or `MIN_LOSS = 0.0`
    # in `config.py`) to keep every event for analyst introspection.
    min_loss:       float = 1000.0

    def vendor(self, name: VendorName) -> Vendor:
        for v in self.vendors:
            if v.name == name:
                return v
        raise KeyError(f"unknown vendor: {name!r}")


def _load_local_config():
    """Import `config.py` from the repo root if it exists, else return None."""
    path = REPO_ROOT / "config.py"
    if not path.exists():
        return None
    spec = importlib.util.spec_from_file_location("_rollup_local_config", path)
    mod  = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception as e:
        raise SystemExit(
            f"error: config.py has a problem: {type(e).__name__}: {e}\n"
            "fix config.py and retry."
        )
    return mod


def resolve() -> Config:
    """Build a `Config` from `config.py` or env vars, falling back to repo defaults.

    Values in `config.py` are used when the corresponding env var is absent.
    Env vars always win (useful for CI overrides).
    """
    _cfg = _load_local_config()

    def _getval(var: EnvVar, attr: str) -> str | None:
        return os.getenv(var) or (getattr(_cfg, attr, None) if _cfg else None)

    data_root = _env_path(EnvVar.DATA_DIR, REPO_ROOT / "data")
    raw_min_loss = _getval(EnvVar.MIN_LOSS, "MIN_LOSS")
    # Use `is not None` so an explicit "0" / 0.0 properly disables the filter
    # — `if raw_min_loss` would treat both as falsy.
    min_loss = float(raw_min_loss) if raw_min_loss is not None else 1000.0
    return Config(
        seeds_dir=_env_path(EnvVar.SEEDS_DIR, data_root / "seeds"),
        output_dir=_env_path(EnvVar.OUTPUT_DIR, data_root / "output"),
        vendors=(_verisk(data_root), _risklink(data_root)),
        mssql_conn_str=_getval(EnvVar.MSSQL_CONN_STR, "MSSQL_CONN_STR"),
        min_loss=min_loss,
    )


# --------------------------------------------------------------------------- #
# Plan                                                                        #
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Check:
    """One input file's status in the plan."""
    label:     str
    path:      Path
    ok:        bool
    rows:      int = 0
    note:      str = ""

    @property
    def mark(self) -> str:
        return "\u2713" if self.ok else "\u2718"


@dataclass(frozen=True)
class Section:
    title:   str
    header:  str              # second line: path + any extra ("n_simulations=...", glob, etc.)
    checks:  list[Check]


@dataclass(frozen=True)
class Plan:
    config:   Config
    sections: list[Section] = field(default_factory=list)

    @property
    def seeds_section(self) -> Section:
        return next(s for s in self.sections if s.title == "seeds")

    @property
    def all_seeds_ok(self) -> bool:
        return all(c.ok for c in self.seeds_section.checks)

    @property
    def all_ylt_ok(self) -> bool:
        """All vendors have at least one YLT file present."""
        for v in self.config.vendors:
            sec = next((s for s in self.sections if s.title == f"ylt {v.name}"), None)
            if sec is None or not any(c.ok for c in sec.checks):
                return False
        return True


# --------------------------------------------------------------------------- #
# Plan construction                                                           #
# --------------------------------------------------------------------------- #

def _check_seed(seeds_dir: Path, spec: SeedSpec) -> Check:
    """Verify a seed: file exists, column headers match, count rows.

    A seed in `REQUIRED_SEEDS` with zero rows is reported as `ok=False` —
    the pipeline would silently produce zero-row Hisco parquets otherwise.
    Non-required seeds (e.g. `air_events`, `fineart_adjustments`) may
    legitimately be empty stubs and are reported `ok=True` with `(stub)`.

    Missing files are reported by seed name. Existing files get a header diff
    before dtype validation, so schema drift remains explicit.
    """
    if not spec.filename:
        return Check(label=spec.name, path=seeds_dir, ok=False, note="missing")
    path = seeds_dir / spec.filename
    if not path.exists():
        return Check(label=spec.name, path=path, ok=False, note="missing")
    try:
        sniff = pl.scan_csv(path).collect_schema().names()
        expected = set(spec.schema.names())
        missing = expected - set(sniff)
        extra   = set(sniff) - expected
        if missing or extra:
            bits = []
            if missing: bits.append(f"missing={sorted(missing)}")
            if extra:   bits.append(f"unexpected={sorted(extra)}")
            return Check(label=spec.name, path=path, ok=False, note=", ".join(bits))
        # Header matches; validate dtypes by reading the first row with the declared
        # schema — this surfaces coercion failures (e.g. "abc" in an Int64 column).
        pl.read_csv(path, schema=spec.schema, n_rows=1)
        rows = pl.scan_csv(path, schema=spec.schema).select(pl.len()).collect().item()
        if rows == 0 and spec.name in REQUIRED_SEEDS:
            return Check(label=spec.name, path=path, ok=False, rows=0,
                         note="REQUIRED seed is empty — pipeline would produce zero-row Hisco parquets")
        note = "schema OK" + (" (stub)" if rows == 0 else "")
        return Check(label=spec.name, path=path, ok=True, rows=rows, note=note)
    except Exception as e:
        return Check(label=spec.name, path=path, ok=False, note=f"parse error: {e}")


def _format_diffs(diffs: list[ColumnDiff]) -> str:
    """Group ColumnDiff entries by kind and produce a compact human-readable string.

    Produces entries like:
      missing=['col1', 'col2'], wrong_dtype=['col3:Float64→Int64'], unexpected=['col4']
    Empty groups are omitted.
    """
    missing     = [str(d.column) for d in diffs if d.kind == "missing"]
    wrong_dtype = [f"{d.column}:{d.detail}" for d in diffs if d.kind == "wrong_dtype"]
    unexpected  = [str(d.column) for d in diffs if d.kind == "unexpected"]
    parts: list[str] = []
    if missing:
        parts.append(f"missing={missing!r}")
    if wrong_dtype:
        parts.append(f"wrong_dtype={wrong_dtype!r}")
    if unexpected:
        parts.append(f"unexpected={unexpected!r}")
    return ", ".join(parts)


def _check_ylt_parquet(path: Path, expected_schema: pl.Schema, name: str) -> Check:
    """Check one YLT parquet: read its schema and compare against expected."""
    try:
        actual = pl.scan_parquet(path).collect_schema()
    except Exception as e:
        return Check(label=name, path=path, ok=False, note=f"parse error: {e}")
    size_mb = path.stat().st_size / 1e6
    diffs = column_diff(actual, expected_schema)
    if not diffs:
        return Check(label=name, path=path, ok=True, note=f"{size_mb:.1f} MB | schema OK")
    return Check(label=name, path=path, ok=False, note=f"{size_mb:.1f} MB | {_format_diffs(diffs)}")


def _check_dir_glob(
    label: str,
    directory: Path,
    glob: str,
    parquet_schema: pl.Schema | None = None,
) -> list[Check]:
    """Generic dir-pattern check used for both YLT parquets and EP-summary CSVs.

    When `parquet_schema` is provided, each `.parquet` file is validated against
    that schema via `_check_ylt_parquet`. Other file types (CSV, xlsx) retain the
    original size-only check.
    """
    if not directory.exists():
        return [Check(label=glob, path=directory, ok=False, note="directory does not exist")]
    files = sorted(directory.glob(glob))
    if not files:
        return [Check(label=glob, path=directory, ok=False, note="no files match pattern")]
    checks: list[Check] = []
    for p in files:
        if parquet_schema is not None and p.suffix == ".parquet":
            checks.append(_check_ylt_parquet(p, parquet_schema, p.name))
        else:
            checks.append(Check(label=p.name, path=p, ok=True, note=f"{p.stat().st_size / 1e6:.1f} MB"))
    return checks


def build_plan(config: Config) -> Plan:
    sections: list[Section] = []

    seed_specs = discover_seeds(config.seeds_dir)
    sections.append(Section(
        title="seeds",
        header=str(config.seeds_dir),
        checks=[_check_seed(config.seeds_dir, spec) for spec in seed_specs],
    ))

    _YLT_SCHEMAS: dict[VendorName, pl.Schema] = {
        VendorName.VERISK:   F.RAW_VERISK_YLT,
        VendorName.RISKLINK: F.RAW_RISKLINK_YLT,
    }

    for vendor in config.vendors:
        sections.append(Section(
            title=f"ylt {vendor.name}",
            header=(f"{vendor.ylt_dir}  "
                    f"pattern={vendor.ylt_glob}  "
                    f"n_simulations={vendor.n_simulations:,}"),
            checks=_check_dir_glob(
                vendor.name,
                vendor.ylt_dir,
                vendor.ylt_glob,
                parquet_schema=_YLT_SCHEMAS.get(vendor.name),
            ),
        ))

    for vendor in config.vendors:
        sections.append(Section(
            title=f"ep_summaries {vendor.name}",
            header=f"{vendor.ep_summary_dir}  pattern={vendor.ep_summary_glob}",
            checks=_check_dir_glob(vendor.name, vendor.ep_summary_dir, vendor.ep_summary_glob),
        ))

    sections.append(Section(
        title="output",
        header=str(config.output_dir),
        checks=[Check(label="output_dir", path=config.output_dir,
                      ok=True, note="will be created on run")],
    ))

    return Plan(config=config, sections=sections)


# --------------------------------------------------------------------------- #
# Pretty printing + interactive confirm                                       #
# --------------------------------------------------------------------------- #

def format_plan(plan: Plan) -> str:
    lines = ["Pipeline plan", "=" * 13, ""]
    for section in plan.sections:
        lines.append(f"[{section.title}]  {section.header}")
        for c in section.checks:
            row = f"  {c.mark} {c.label:<30}"
            if c.rows:
                row += f"  {c.rows:>8,} rows"
            else:
                row += "  " + " " * 12
            if c.note:
                row += f"   {c.note}"
            lines.append(row.rstrip())
        lines.append("")

    # Summary
    seed_ok = sum(1 for c in plan.seeds_section.checks if c.ok)
    seed_total = len(plan.seeds_section.checks)
    ylt_ready = sum(
        1 for v in plan.config.vendors
        if any(c.ok for s in plan.sections if s.title == f"ylt {v.name}" for c in s.checks)
    )
    ep_ready = sum(
        1 for v in plan.config.vendors
        if any(c.ok for s in plan.sections if s.title == f"ep_summaries {v.name}" for c in s.checks)
    )
    lines.append(f"Seeds: {seed_ok}/{seed_total} valid.")
    lines.append(f"YLTs:  {ylt_ready}/{len(plan.config.vendors)} vendors have data.")
    lines.append(f"EP summaries: {ep_ready}/{len(plan.config.vendors)} vendors have data.")
    if plan.config.mssql_conn_str:
        lines.append(f"SQL Server: {redact_conn_str(plan.config.mssql_conn_str)}")
    else:
        lines.append("SQL Server: not configured (parquet-only run)")
    lines.append("")
    return "\n".join(lines)


def redact_conn_str(conn_str: str) -> str:
    """Hide `user:pass@` if present in a `scheme://user:pass@host/...` URL.

    Windows-auth ODBC strings have no credentials inline; passes through.
    Public API — imported by cli.py and available for external callers.
    """
    if "://" not in conn_str:
        return conn_str
    scheme, rest = conn_str.split("://", 1)
    if "@" in rest and not rest.startswith("@"):
        rest = f"...@{rest.split('@', 1)[1]}"
    return f"{scheme}://{rest}"


def _section_icon(title: str) -> str:
    """Return the icon for a section title; falls back to '·' for unknowns."""
    for key, icon in _SECTION_ICONS.items():
        if title.startswith(key):
            return icon
    return "·"


def _status_pill(ok: int, total: int) -> Text:
    """Right-side status pill: '12/12 ✓' coloured by completeness."""
    if total == 0:
        return Text(f"{_GLYPH_FAIL} empty", style=_FAIL)
    if ok == total:
        return Text(f"{ok}/{total} {_GLYPH_OK}", style=_OK)
    if ok == 0:
        return Text(f"{ok}/{total} {_GLYPH_FAIL}", style=_FAIL)
    return Text(f"{ok}/{total} {_GLYPH_WARN}", style=_WARN)


def _render_section_header(section: Section, console_width: int) -> Table:
    """One-line section header: icon + title + path on the left, status pill on the right.

    The left column ellipsizes when the path is long so the pill always
    has clear space (no jamming against truncated text).
    """
    icon = _section_icon(section.title)
    ok = sum(1 for c in section.checks if c.ok)
    total = len(section.checks)

    head = Table(show_header=False, box=None, expand=True, pad_edge=False, padding=(0, 0))
    head.add_column(no_wrap=True, ratio=1, overflow="ellipsis")
    head.add_column(no_wrap=True, justify="right", min_width=10)

    left = Text.assemble(
        (icon, _LABEL),
        ("  ", ""),
        (section.title, _LABEL),
        ("    ", ""),
        (section.header, _DIM),
        ("  ", ""),    # gap before the pill, even when truncated
    )
    head.add_row(left, _status_pill(ok, total))
    return head


def _render_check_table(checks: list[Check]) -> Table:
    """Indented per-file table with glyph, label, row count, note.

    Row-count column is only present when at least one check has rows,
    keeping the YLT/EP-summary tables tighter.
    """
    has_rows = any(c.rows for c in checks)

    tbl = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1), expand=False)
    tbl.add_column(width=1, no_wrap=True)                                 # glyph
    tbl.add_column(min_width=24, max_width=44, overflow="fold")           # label
    if has_rows:
        tbl.add_column(justify="right", min_width=10, no_wrap=True)       # rows
    tbl.add_column(overflow="fold", no_wrap=False)                        # note

    for c in checks:
        glyph = _GLYPH_OK if c.ok else _GLYPH_FAIL
        glyph_style = _OK if c.ok else _FAIL
        label_style = _BODY if c.ok else _FAIL
        note_style = _DIM if c.ok else _FAIL

        cells = [
            Text(glyph, style=glyph_style),
            Text(c.label, style=label_style),
        ]
        if has_rows:
            rows_text = f"{c.rows:>7,} rows" if c.rows else ""
            cells.append(Text(rows_text, style=_NUM))
        cells.append(Text(c.note, style=note_style))
        tbl.add_row(*cells)
    return tbl


def _final_summary_line(plan: Plan) -> Text:
    """One-line horizontal summary: pills separated by faint vertical bars."""
    seed_ok = sum(1 for c in plan.seeds_section.checks if c.ok)
    seed_total = len(plan.seeds_section.checks)
    n_vendors = len(plan.config.vendors)
    ylt_ready = sum(
        1 for v in plan.config.vendors
        if any(c.ok for s in plan.sections if s.title == f"ylt {v.name}" for c in s.checks)
    )
    ep_ready = sum(
        1 for v in plan.config.vendors
        if any(c.ok for s in plan.sections if s.title == f"ep_summaries {v.name}" for c in s.checks)
    )

    parts: list[Text] = []

    def _add(label: str, pill: Text) -> None:
        if parts:
            parts.append(Text("  │  ", style=_DIM))
        parts.append(Text.assemble((label, _DIM), ("  ", ""), pill))

    _add("seeds",  _status_pill(seed_ok, seed_total))
    _add("ylt",    _status_pill(ylt_ready, n_vendors))
    _add("ep",     _status_pill(ep_ready, n_vendors))

    if plan.config.mssql_conn_str:
        _add("sql", Text(redact_conn_str(plan.config.mssql_conn_str), style=_BODY))
    else:
        _add("sql", Text(f"{_GLYPH_FAIL} not configured", style=_DIM))

    out = Text()
    for p in parts:
        out.append_text(p)
    return out


def print_plan(plan: Plan, console: Console | None = None) -> None:
    """Render the plan with Rich — colour, rules, glyphs, pills.

    For non-tty / piped output use `format_plan(plan)` instead (returns a
    plain string). Tests compare against `format_plan` so they're unaffected.
    """
    if console is None:
        console = Console()

    width = console.width or 100

    # Hero rule with title.
    title = Text.assemble(
        ("  polars rollup pipeline  ", _BRAND),
        ("·  pre-flight plan  ", _DIM),
    )
    console.print()
    console.print(Rule(title, style=_RULE))
    console.print()

    # Each section: header line (with right-aligned pill), then indented table.
    for i, section in enumerate(plan.sections):
        console.print(_render_section_header(section, width))
        if section.checks:
            console.print(Padding(_render_check_table(section.checks), (0, 0, 0, 4)))
        if i < len(plan.sections) - 1:
            console.print()

    # Footer rule + one-line summary.
    console.print()
    console.print(Rule(style=_RULE))
    console.print(Padding(_final_summary_line(plan), (0, 2)))
    console.print()


def confirm(plan: Plan, *, assume_yes: bool = False, stream=sys.stdout) -> bool:
    """Print the plan, ask y/N. Returns True if the user accepts.

    `assume_yes=True` skips the prompt (for CI and tests). Non-interactive
    stdin (`stdin.isatty() is False`) also returns True — callers that need
    strict confirmation in pipes should check `sys.stdin.isatty()` themselves.

    Uses the Rich renderer when the stream is a tty; falls back to the
    plain `format_plan` for piped / non-tty output (tests, CI, file dumps).
    """
    use_rich = getattr(stream, "isatty", lambda: False)() and stream is sys.stdout
    if use_rich:
        print_plan(plan, console=Console(file=stream))
    else:
        print(format_plan(plan), file=stream)

    if not plan.all_seeds_ok:
        if use_rich:
            Console(file=stream).print(
                Text("! seeds have errors — fix before running.", style=_FAIL)
            )
        else:
            print("! seeds have errors — fix before running.", file=stream)
    if assume_yes:
        if use_rich:
            Console(file=stream).print(Text("(--yes) proceeding", style=_OK))
        else:
            print("(--yes) proceeding", file=stream)
        return True
    if not sys.stdin.isatty():
        print("(non-interactive stdin) proceeding", file=stream)
        return True
    try:
        prompt = "Proceed? [y/N]: "
        if use_rich:
            console = Console(file=stream)
            console.print(Text(prompt, style=_BRAND), end="")
            reply = input().strip().lower()
        else:
            reply = input(prompt).strip().lower()
    except EOFError:
        return False
    return reply in {"y", "yes"}
