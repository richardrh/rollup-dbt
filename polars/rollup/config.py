"""Pipeline configuration: vendors, paths, and the pre-run plan reporter.

One place that answers four questions:

  1. Who are the vendors, and how many simulation years does each have?
     → `VERISK`, `RISKLINK` (both `Vendor` instances), `VENDORS`.
  2. Where does data live on disk?
     → `Config.seeds_dir`, `ep_summaries_dir`, `output_dir`, and each
     vendor's `ylt_dir` + `ylt_glob`.
  3. Are all the required inputs present and schema-valid?
     → `build_plan(config)` returns a `Plan` with per-file status.
  4. Does the user want to run?
     → `confirm(plan, assume_yes=bool)` prints + prompts.

Default layout:

    <repo>/
    ├── polars/
    │   ├── rollup/              ← this package
    │   └── seeds/               ← versioned reference CSVs
    └── data/                    ← NOT in git; populated by the user
        ├── ylt/
        │   ├── verisk/*.parquet (≈ 10 000 simulation years)
        │   └── risklink/*.parquet (≈ 100 000 simulation years)
        ├── ep_summaries/
        │   ├── verisk/*.csv
        │   └── risklink/*.csv
        └── output/              ← Hisco parquets written here

Override any path with the corresponding `ROLLUP_*` env var.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

import polars as pl

from rollup.seeds import SEEDS


# --------------------------------------------------------------------------- #
# Fan-out flavours                                                            #
# --------------------------------------------------------------------------- #

class Flavor(StrEnum):
    """The minimum-viable Hisco flavour set.

    january had 21 fan-out variants including a tree of `_fix` / `_fl_fa_fix`
    patch names. Those were workarounds for math that wasn't quite right;
    with a clean pipeline we only need three semantic flavours:

      * STANDARD — the default output: capped, local-ccy, forecast-adjusted,
                   euws-adjusted. Does NOT apply fine-art gross-to-net.
      * FAGROSS  — STANDARD plus the fine-art gross-to-net adjustment.
      * DIALSUP  — sensitivity scenario: the composite forecast × euws ×
                   fa_gross factor applied directly to the RAW loss,
                   bypassing uplift / cap / FX.

    Per vendor × forecast_date, we emit one of each by default. Flavours
    can be added or removed by editing the `Vendor.flavors` field.
    """
    STANDARD = "standard"
    FAGROSS  = "fagross"
    DIALSUP  = "dialsup"


POLARS_ROOT = Path(__file__).resolve().parent.parent    # .../polars
REPO_ROOT   = POLARS_ROOT.parent                        # .../rollup-dbt


# --------------------------------------------------------------------------- #
# Vendors                                                                     #
# --------------------------------------------------------------------------- #

_DEFAULT_FLAVORS: tuple[Flavor, ...] = (Flavor.STANDARD, Flavor.FAGROSS, Flavor.DIALSUP)


@dataclass(frozen=True)
class Vendor:
    """Everything that varies by vendor, in one place.

    `name` is the string that appears in the YLT `vendor` column throughout
    the pipeline. `hisco_label` only appears in output filenames
    (`HiscoAIR_*` / `HiscoRMS_*`) — these are the external contract and
    must stay as-is.

    `flavors` declares which Hisco flavours this vendor produces. Per
    vendor × forecast_date (from the `forecast_factors` seed) × flavor
    we emit one Hisco parquet.
    """
    name:            str              # "verisk" | "risklink"
    hisco_label:     str              # "AIR" | "RMS" — output filename prefix
    n_simulations:   int              # 10_000 | 100_000
    ylt_dir:         Path
    ylt_glob:        str
    ep_summary_dir:  Path
    ep_summary_glob: str              = "*.csv"
    flavors:         tuple[Flavor, ...] = _DEFAULT_FLAVORS


def _env_path(var: str, default: Path) -> Path:
    raw = os.getenv(var)
    return Path(raw).expanduser().resolve() if raw else default


def _verisk(data_root: Path) -> Vendor:
    return Vendor(
        name="verisk",
        hisco_label="AIR",
        n_simulations=10_000,
        ylt_dir=_env_path("ROLLUP_YLT_VERISK_DIR", data_root / "ylt" / "verisk"),
        ylt_glob=os.getenv("ROLLUP_YLT_VERISK_GLOB", "air_ylt_*.parquet"),
        ep_summary_dir=_env_path("ROLLUP_EP_VERISK_DIR", data_root / "ep_summaries" / "verisk"),
    )


def _risklink(data_root: Path) -> Vendor:
    return Vendor(
        name="risklink",
        hisco_label="RMS",
        n_simulations=100_000,
        ylt_dir=_env_path("ROLLUP_YLT_RISKLINK_DIR", data_root / "ylt" / "risklink"),
        ylt_glob=os.getenv("ROLLUP_YLT_RISKLINK_GLOB", "risklink_ylt_*.parquet"),
        ep_summary_dir=_env_path("ROLLUP_EP_RISKLINK_DIR", data_root / "ep_summaries" / "risklink"),
    )


# --------------------------------------------------------------------------- #
# Top-level config                                                            #
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Config:
    seeds_dir:  Path
    output_dir: Path
    vendors:    tuple[Vendor, ...]

    def vendor(self, name: str) -> Vendor:
        for v in self.vendors:
            if v.name == name:
                return v
        raise KeyError(f"unknown vendor: {name!r}")


def resolve() -> Config:
    """Build a `Config` from env vars, falling back to repo defaults."""
    data_root = _env_path("ROLLUP_DATA_DIR", REPO_ROOT / "data")
    return Config(
        seeds_dir=_env_path("ROLLUP_SEEDS_DIR", POLARS_ROOT / "seeds"),
        output_dir=_env_path("ROLLUP_OUTPUT_DIR", data_root / "output"),
        vendors=(_verisk(data_root), _risklink(data_root)),
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
    """Verify a seed: file exists, column headers match, count rows."""
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
        note = "schema OK" + (" (stub)" if rows == 0 else "")
        return Check(label=spec.name, path=path, ok=True, rows=rows, note=note)
    except Exception as e:
        return Check(label=spec.name, path=path, ok=False, note=f"parse error: {e}")


def _check_ylt(vendor: Vendor) -> list[Check]:
    if not vendor.ylt_dir.exists():
        return [Check(label=vendor.ylt_glob, path=vendor.ylt_dir, ok=False,
                      note="directory does not exist")]
    files = sorted(vendor.ylt_dir.glob(vendor.ylt_glob))
    if not files:
        return [Check(label=vendor.ylt_glob, path=vendor.ylt_dir, ok=False,
                      note="no files match pattern")]
    return [Check(label=p.name, path=p, ok=True,
                  note=f"{p.stat().st_size / 1e6:.1f} MB") for p in files]


def _check_ep_summary(vendor: Vendor) -> list[Check]:
    if not vendor.ep_summary_dir.exists():
        return [Check(label=vendor.ep_summary_glob, path=vendor.ep_summary_dir, ok=False,
                      note="directory does not exist")]
    files = sorted(vendor.ep_summary_dir.glob(vendor.ep_summary_glob))
    if not files:
        return [Check(label=vendor.ep_summary_glob, path=vendor.ep_summary_dir, ok=False,
                      note="no files match pattern")]
    return [Check(label=p.name, path=p, ok=True) for p in files]


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
            checks=_check_ylt(vendor),
        ))

    for vendor in config.vendors:
        sections.append(Section(
            title=f"ep_summaries {vendor.name}",
            header=f"{vendor.ep_summary_dir}  pattern={vendor.ep_summary_glob}",
            checks=_check_ep_summary(vendor),
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
