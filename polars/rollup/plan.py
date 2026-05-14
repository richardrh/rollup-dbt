"""Pre-run input checks and plan construction."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from rollup.config import Config, VendorName
from rollup.schemas import frames as F
from rollup.schemas.columns import AnalysesCol as AN
from rollup.schemas.columns import RefForecastFactorsCol as FF
from rollup.schemas.columns import RefLobsCol as LB
from rollup.schemas.columns import ValidAnalysesCol as VA
from rollup.seeds import REQUIRED_SEEDS, SeedSpec, discover as discover_seeds, load_seed_file
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

    @property
    def all_ep_ok(self) -> bool:
        """All vendors have at least one EP-summary long CSV/file present."""
        for vendor in self.config.vendors:
            section = next((s for s in self.sections if s.title == f"ep_summaries {vendor.name}"), None)
            if section is None or not any(c.ok for c in section.checks):
                return False
        return True

    @property
    def all_lob_peril_ok(self) -> bool:
        """No rollup LOB maps to more than one peril in valid analysis metadata."""
        section = next((s for s in self.sections if s.title == "lob_peril_validation"), None)
        if section is None:
            return True
        return all(c.ok for c in section.checks)

    @property
    def has_lob_peril_conflict(self) -> bool:
        """The one-peril-per-rollup-lob check found an actual conflict."""
        section = next((s for s in self.sections if s.title == "lob_peril_validation"), None)
        if section is None:
            return False
        return any((not c.ok) and "validation failed" in c.note for c in section.checks)


def _check_seed(seeds_dir: Path, spec: SeedSpec) -> Check:
    """Verify a seed: file exists, column headers match, count rows.

    A seed in `REQUIRED_SEEDS` with zero rows is reported as `ok=False` —
    the pipeline would silently produce zero-row Hisco parquets otherwise.
    Non-required seeds (e.g. `air_events`) may
    legitimately be empty stubs and are reported `ok=True` with `(stub)`.
    """
    if not spec.filename:
        return Check(label=spec.name, path=seeds_dir, ok=False, note="missing")
    path = seeds_dir / spec.filename
    if not path.exists():
        return Check(label=spec.name, path=path, ok=False, note="missing")
    try:
        if path.suffix != ".parquet":
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
        lf = load_seed_file(path, spec.schema, name=spec.name)
        rows = lf.select(pl.len()).collect().item()
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
    blocking_diffs = [d for d in diffs if d.kind != "unexpected"]
    if not blocking_diffs:
        return Check(label=name, path=path, ok=True, note=f"{size_mb:.1f} MB | schema OK")
    return Check(label=name, path=path, ok=False, note=f"{size_mb:.1f} MB | {_format_diffs(diffs)}")


def _check_dir_glob(
    label: str,
    directory: Path,
    glob: str,
    parquet_schema: pl.Schema | None = None,
    optional_missing_ok: bool = False,
    missing_note: str = "no files match pattern",
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
        return [Check(label=glob, path=directory, ok=False, note=missing_note)]
    checks: list[Check] = []
    for path in files:
        if parquet_schema is not None and path.suffix == ".parquet":
            checks.append(_check_ylt_parquet(path, parquet_schema, path.name))
        else:
            checks.append(Check(label=path.name, path=path, ok=True, note=f"{path.stat().st_size / 1e6:.1f} MB"))
    return checks


def _check_forecast_coverage(seeds_dir: Path) -> list[Check]:
    """Report forecast dates and coverage for scoped LOB/analysis rows.

    Forecast factors join at runtime on ``(office, class)``. Missing rows are
    currently filled as factor 1.0, so the plan surfaces coverage gaps before
    the operator starts a run.
    """
    lobs_path = seeds_dir / "business" / "lobs.csv"
    valid_path = seeds_dir / "business" / "valid_analyses.csv"
    ff_path = seeds_dir / "vor" / "forecast_factors.csv"
    if not lobs_path.exists() or not valid_path.exists() or not ff_path.exists():
        return [Check(label="forecast coverage", path=seeds_dir, ok=False, note="missing seed input")]

    try:
        lobs = pl.read_csv(lobs_path, schema=F.REF_LOBS)
        valid_analyses = pl.read_csv(valid_path, schema=F.VALID_ANALYSES)
        forecast = pl.read_csv(ff_path, schema=F.REF_FORECAST_FACTORS)
    except Exception as e:
        return [Check(label="forecast coverage", path=ff_path, ok=False, note=f"parse error: {e}")]

    dates = forecast.select(FF.FORECAST_DATE).unique().sort(FF.FORECAST_DATE)
    if dates.height == 0:
        return [Check(label="forecast dates", path=ff_path, ok=False, note="no forecast dates found")]

    date_labels = [d.isoformat() for d in dates[FF.FORECAST_DATE].to_list()]
    checks = [
        Check(
            label="forecast dates",
            path=ff_path,
            ok=True,
            rows=dates.height,
            note=", ".join(date_labels),
        )
    ]

    if valid_analyses.height == 0:
        return checks + [Check(label="forecast coverage", path=valid_path, ok=False, note="valid_analyses is empty")]

    scoped_lobs = lobs.select(LB.MODELLED_LOB, LB.OFFICE, LB.CLASS).unique()
    expected = scoped_lobs.join(dates, how="cross")
    actual = forecast.select(FF.OFFICE, FF.CLASS, FF.FORECAST_DATE).unique()
    missing = expected.join(
        actual,
        left_on=[LB.OFFICE, LB.CLASS, FF.FORECAST_DATE],
        right_on=[FF.OFFICE, FF.CLASS, FF.FORECAST_DATE],
        how="anti",
    )

    if missing.height == 0:
        checks.append(Check(
            label="forecast coverage",
            path=ff_path,
            ok=True,
            rows=expected.height,
            note="all modelled_lob rows covered for every forecast date",
        ))
        return checks

    examples = missing.select(
        LB.MODELLED_LOB,
        LB.OFFICE,
        LB.CLASS,
        FF.FORECAST_DATE,
    ).head(5)
    example_text = "; ".join(
        f"{row[LB.MODELLED_LOB]} {row[LB.OFFICE]}/{row[LB.CLASS]} {row[FF.FORECAST_DATE]}"
        for row in examples.iter_rows(named=True)
    )
    return checks + [Check(
        label="forecast coverage",
        path=ff_path,
        ok=True,
        rows=missing.height,
        note=f"WARNING missing factors for scoped rows; examples: {example_text}",
    )]


def _check_one_peril_per_rollup_lob(seeds_dir: Path) -> list[Check]:
    """Validate valid RiskLink analysis IDs keep one analysis per LOB/peril.

    RiskLink analysis metadata carries ``lob_id``, so this seed-only check can
    catch operator allow-list mistakes before a run starts. Verisk LOBs live in
    the YLT row and remain covered by the runtime staging validation.
    """
    lobs_path = seeds_dir / "business" / "lobs.csv"
    analyses_path = seeds_dir / "business" / "analyses.csv"
    valid_path = seeds_dir / "business" / "valid_analyses.csv"
    if not lobs_path.exists() or not analyses_path.exists() or not valid_path.exists():
        return [Check(label="one analysis per lob/peril", path=seeds_dir, ok=False, note="missing seed input")]

    try:
        lobs = pl.read_csv(lobs_path, schema=F.REF_LOBS)
        analyses = pl.read_csv(analyses_path, schema=F.ANALYSES)
        valid_analyses = pl.read_csv(valid_path, schema=F.VALID_ANALYSES)
    except Exception as e:
        return [Check(label="one analysis per lob/peril", path=valid_path, ok=False, note=f"parse error: {e}")]

    mapped = (
        analyses
        .join(
            valid_analyses.unique(),
            left_on=[AN.VENDOR, AN.ANALYSIS_ID],
            right_on=[VA.VENDOR, VA.ANALYSIS_ID],
            how="inner",
        )
        .filter(pl.col(AN.LOB_ID).is_not_null())
        .filter(pl.col(AN.LOB_ID).is_not_null())
        .select(AN.LOB_ID, AN.PERIL_ID, AN.ANALYSIS_ID)
        .unique()
    )
    if mapped.height == 0:
        return [Check(
            label="one analysis per lob/peril",
            path=valid_path,
            ok=True,
            note="no lob-specific valid analyses to validate",
        )]

    conflicts = (
        mapped
        .group_by(AN.LOB_ID, AN.PERIL_ID)
        .agg(
            pl.col(AN.ANALYSIS_ID).sort().alias("analysis_ids"),
            pl.len().alias("n_analyses"),
        )
        .filter(pl.col("n_analyses") > 1)
    )
    if conflicts.height == 0:
        return [Check(
            label="one analysis per lob/peril",
            path=valid_path,
            ok=True,
            rows=mapped.select(AN.LOB_ID, AN.PERIL_ID).unique().height,
            note="valid analysis metadata maps each LOB/peril to one analysis",
        )]

    examples = "; ".join(
        f"lob={row[AN.LOB_ID]} peril={row[AN.PERIL_ID]} -> {row['analysis_ids']}"
        for row in conflicts.head(5).iter_rows(named=True)
    )
    return [Check(
        label="one analysis per lob/peril",
        path=valid_path,
        ok=True,
        rows=conflicts.height,
        note=f"WARNING duplicate valid analyses for LOB/peril; examples: {examples}",
    )]


def build_plan(config: Config, *, require_ep_summaries: bool = False) -> Plan:
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
        ep_glob = "*.long.csv" if require_ep_summaries else vendor.ep_summary_glob
        sections.append(Section(
            title=f"ep_summaries {vendor.name}",
            header=f"{vendor.ep_summary_dir}  pattern={ep_glob}",
            checks=_check_dir_glob(
                vendor.name,
                vendor.ep_summary_dir,
                ep_glob,
                optional_missing_ok=not require_ep_summaries,
                missing_note="optional; use ep-summary-to-csv to generate long CSVs for review",
            ),
        ))

    sections.append(Section(
        title="lob_peril_validation",
        header=str(config.seeds_dir / "business" / "valid_analyses.csv"),
        checks=_check_one_peril_per_rollup_lob(config.seeds_dir),
    ))

    sections.append(Section(
        title="forecast_factors",
        header=str(config.seeds_dir / "vor" / "forecast_factors.csv"),
        checks=_check_forecast_coverage(config.seeds_dir),
    ))

    sections.append(Section(
        title="output",
        header=str(config.output_dir),
        checks=[Check(label="output_dir", path=config.output_dir, ok=True, note="will be created on run")],
    ))

    return Plan(config=config, sections=sections)
