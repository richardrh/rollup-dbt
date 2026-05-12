"""Pre-run input checks and plan construction."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from rollup.config import Config, VendorName
from rollup.schemas import frames as F
from rollup.seeds import REQUIRED_SEEDS, SeedSpec, discover as discover_seeds
from rollup.validate import ColumnDiff, column_diff


@dataclass(frozen=True)
class Check:
    """One input file's status in the plan."""
    label: str
    path: Path
    ok: bool
    rows: int = 0
    note: str = ""

    @property
    def mark(self) -> str:
        return "✓" if self.ok else "✘"


@dataclass(frozen=True)
class Section:
    title: str
    header: str
    checks: list[Check]


@dataclass(frozen=True)
class Plan:
    config: Config
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
        for vendor in self.config.vendors:
            section = next((s for s in self.sections if s.title == f"ylt {vendor.name}"), None)
            if section is None or not any(c.ok for c in section.checks):
                return False
        return True


def _check_seed(seeds_dir: Path, spec: SeedSpec) -> Check:
    """Verify a seed: file exists, column headers match, count rows.

    A seed in `REQUIRED_SEEDS` with zero rows is reported as `ok=False` —
    the pipeline would silently produce zero-row Hisco parquets otherwise.
    Non-required seeds (e.g. `air_events`, `fineart_adjustments`) may
    legitimately be empty stubs and are reported `ok=True` with `(stub)`.
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
        extra = set(sniff) - expected
        if missing or extra:
            bits = []
            if missing:
                bits.append(f"missing={sorted(missing)}")
            if extra:
                bits.append(f"unexpected={sorted(extra)}")
            return Check(label=spec.name, path=path, ok=False, note=", ".join(bits))

        pl.read_csv(path, schema=spec.schema, n_rows=1)
        rows = pl.scan_csv(path, schema=spec.schema).select(pl.len()).collect().item()
        if rows == 0 and spec.name in REQUIRED_SEEDS:
            return Check(
                label=spec.name,
                path=path,
                ok=False,
                rows=0,
                note="REQUIRED seed is empty — pipeline would produce zero-row Hisco parquets",
            )
        note = "schema OK" + (" (stub)" if rows == 0 else "")
        return Check(label=spec.name, path=path, ok=True, rows=rows, note=note)
    except Exception as e:
        return Check(label=spec.name, path=path, ok=False, note=f"parse error: {e}")


def _format_diffs(diffs: list[ColumnDiff]) -> str:
    """Group ColumnDiff entries by kind and produce a compact string."""
    missing = [str(d.column) for d in diffs if d.kind == "missing"]
    wrong_dtype = [f"{d.column}:{d.detail}" for d in diffs if d.kind == "wrong_dtype"]
    unexpected = [str(d.column) for d in diffs if d.kind == "unexpected"]
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
    optional_missing_ok: bool = False,
) -> list[Check]:
    """Generic dir-pattern check for YLT parquets and EP-summary files."""
    if not directory.exists():
        if optional_missing_ok:
            return [Check(label=glob, path=directory, ok=True, note="optional; directory does not exist")]
        return [Check(label=glob, path=directory, ok=False, note="directory does not exist")]
    files = sorted(directory.glob(glob))
    if not files:
        if optional_missing_ok:
            return [Check(label=glob, path=directory, ok=True, note="optional; using blending_weights seed")]
        return [Check(label=glob, path=directory, ok=False, note="no files match pattern")]
    checks: list[Check] = []
    for path in files:
        if parquet_schema is not None and path.suffix == ".parquet":
            checks.append(_check_ylt_parquet(path, parquet_schema, path.name))
        else:
            checks.append(Check(label=path.name, path=path, ok=True, note=f"{path.stat().st_size / 1e6:.1f} MB"))
    return checks


def build_plan(config: Config) -> Plan:
    sections: list[Section] = []

    seed_specs = discover_seeds(config.seeds_dir)
    sections.append(Section(
        title="seeds",
        header=str(config.seeds_dir),
        checks=[_check_seed(config.seeds_dir, spec) for spec in seed_specs],
    ))

    ylt_schemas: dict[VendorName, pl.Schema] = {
        VendorName.VERISK: F.RAW_VERISK_YLT,
        VendorName.RISKLINK: F.RAW_RISKLINK_YLT,
    }

    for vendor in config.vendors:
        sections.append(Section(
            title=f"ylt {vendor.name}",
            header=(
                f"{vendor.ylt_dir}  "
                f"pattern={vendor.ylt_glob}  "
                f"n_simulations={vendor.n_simulations:,}"
            ),
            checks=_check_dir_glob(
                vendor.name,
                vendor.ylt_dir,
                vendor.ylt_glob,
                parquet_schema=ylt_schemas.get(vendor.name),
            ),
        ))

    for vendor in config.vendors:
        sections.append(Section(
            title=f"ep_summaries {vendor.name}",
            header=f"{vendor.ep_summary_dir}  pattern={vendor.ep_summary_glob}",
            checks=_check_dir_glob(
                vendor.name,
                vendor.ep_summary_dir,
                vendor.ep_summary_glob,
                optional_missing_ok=True,
            ),
        ))

    sections.append(Section(
        title="output",
        header=str(config.output_dir),
        checks=[Check(label="output_dir", path=config.output_dir, ok=True, note="will be created on run")],
    ))

    return Plan(config=config, sections=sections)
