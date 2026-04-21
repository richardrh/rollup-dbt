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

Override any path with the corresponding `ROLLUP_*` env var.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

import polars as pl

from rollup.seeds import REQUIRED_SEEDS, SEEDS


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

    def vendor(self, name: VendorName) -> Vendor:
        for v in self.vendors:
            if v.name == name:
                return v
        raise KeyError(f"unknown vendor: {name!r}")


def resolve() -> Config:
    """Build a `Config` from env vars, falling back to repo defaults."""
    data_root = _env_path(EnvVar.DATA_DIR, REPO_ROOT / "data")
    return Config(
        seeds_dir=_env_path(EnvVar.SEEDS_DIR, data_root / "seeds"),
        output_dir=_env_path(EnvVar.OUTPUT_DIR, data_root / "output"),
        vendors=(_verisk(data_root), _risklink(data_root)),
        mssql_conn_str=os.getenv(EnvVar.MSSQL_CONN_STR),
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


# --------------------------------------------------------------------------- #
# Plan construction                                                           #
# --------------------------------------------------------------------------- #

def _check_seed(seeds_dir: Path, spec) -> Check:
    """Verify a seed: file exists, column headers match, count rows.

    A seed in `REQUIRED_SEEDS` with zero rows is reported as `ok=False` —
    the pipeline would silently produce zero-row Hisco parquets otherwise.
    Non-required seeds (e.g. `air_events`, `fineart_adjustments`) may
    legitimately be empty stubs and are reported `ok=True` with `(stub)`.
    """
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
        rows = pl.scan_csv(path, schema=spec.schema).select(pl.len()).collect().item()
        if rows == 0 and spec.name in REQUIRED_SEEDS:
            return Check(label=spec.name, path=path, ok=False, rows=0,
                         note="REQUIRED seed is empty — pipeline would produce zero-row Hisco parquets")
        note = "schema OK" + (" (stub)" if rows == 0 else "")
        return Check(label=spec.name, path=path, ok=True, rows=rows, note=note)
    except Exception as e:
        return Check(label=spec.name, path=path, ok=False, note=f"parse error: {e}")


def _check_dir_glob(label: str, directory: Path, glob: str) -> list[Check]:
    """Generic dir-pattern check used for both YLT parquets and EP-summary CSVs."""
    if not directory.exists():
        return [Check(label=glob, path=directory, ok=False, note="directory does not exist")]
    files = sorted(directory.glob(glob))
    if not files:
        return [Check(label=glob, path=directory, ok=False, note="no files match pattern")]
    return [
        Check(label=p.name, path=p, ok=True, note=f"{p.stat().st_size / 1e6:.1f} MB")
        for p in files
    ]


def build_plan(config: Config) -> Plan:
    sections: list[Section] = []

    sections.append(Section(
        title="seeds",
        header=str(config.seeds_dir),
        checks=[_check_seed(config.seeds_dir, spec) for spec in SEEDS],
    ))

    for vendor in config.vendors:
        sections.append(Section(
            title=f"ylt {vendor.name}",
            header=(f"{vendor.ylt_dir}  "
                    f"pattern={vendor.ylt_glob}  "
                    f"n_simulations={vendor.n_simulations:,}"),
            checks=_check_dir_glob(vendor.name, vendor.ylt_dir, vendor.ylt_glob),
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
        # Redact credentials from display: show up to the @ only
        display = plan.config.mssql_conn_str
        if "@" in display:
            scheme, rest = display.split("://", 1)
            display = f"{scheme}://...@{rest.split('@', 1)[1]}"
        lines.append(f"SQL Server: {display}")
    else:
        lines.append("SQL Server: not configured (parquet-only run)")
    lines.append("")
    return "\n".join(lines)


def confirm(plan: Plan, *, assume_yes: bool = False, stream=sys.stdout) -> bool:
    """Print the plan, ask y/N. Returns True if the user accepts.

    `assume_yes=True` skips the prompt (for CI and tests). Non-interactive
    stdin (`stdin.isatty() is False`) also returns True — callers that need
    strict confirmation in pipes should check `sys.stdin.isatty()` themselves.
    """
    print(format_plan(plan), file=stream)
    if not plan.all_seeds_ok:
        print("! seeds have errors — fix before running.", file=stream)
    if assume_yes:
        print("(--yes) proceeding", file=stream)
        return True
    if not sys.stdin.isatty():
        print("(non-interactive stdin) proceeding", file=stream)
        return True
    try:
        reply = input("Proceed? [y/N]: ").strip().lower()
    except EOFError:
        return False
    return reply in {"y", "yes"}
