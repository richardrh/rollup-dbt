"""Pre-run input checks and plan construction."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from rollup.config import Config, VendorName
from rollup.analysis_scope import (
    analyses_with_selected_ids_for_run,
    has_selected_analyses_seed,
    selected_analyses_path,
)
from rollup.schemas import frames as F
from rollup.schemas.columns import AnalysesCol as AN
from rollup.schemas.columns import BlendingWeightsCol as BW
from rollup.schemas.columns import CanonicalEpSummaryCol as EP
from rollup.schemas.columns import PerilsCol as PR
from rollup.schemas.columns import RawRisklinkYltCol as RLK
from rollup.schemas.columns import RawVeriskYltCol as VK
from rollup.schemas.columns import RefForecastFactorsCol as FF
from rollup.schemas.columns import RefLobsCol as LB
from rollup.schemas.columns import SelectedAnalysesCol as SA
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
        """All vendors have at least one valid canonical EP-summary CSV."""
        for vendor in self.config.vendors:
            if not self.ep_vendor_ready(vendor.name):
                return False
        return True

    def ep_vendor_ready(self, vendor_name: VendorName) -> bool:
        """A vendor is EP-ready only when canonical CSV checks all pass."""
        section = next((s for s in self.sections if s.title == f"ep_summaries {vendor_name}"), None)
        if section is None or any(not c.ok for c in section.checks):
            return False
        return any(c.path.suffix == ".csv" and c.ok for c in section.checks)

    @property
    def all_selected_analysis_ok(self) -> bool:
        """Selected-analysis workflow checks all pass, or compatibility path is active."""
        section = next((s for s in self.sections if s.title == "selected_analysis_validation"), None)
        if section is None:
            return True
        return all(c.ok for c in section.checks)

    @property
    def has_selected_analysis_conflict(self) -> bool:
        """Selected-analysis validation has at least one blocking failure."""
        section = next((s for s in self.sections if s.title == "selected_analysis_validation"), None)
        if section is None:
            return False
        return any(not c.ok for c in section.checks)

    @property
    def all_lob_peril_ok(self) -> bool:
        """Valid analysis metadata selects at most one analysis per LOB/peril."""
        section = next((s for s in self.sections if s.title == "lob_peril_validation"), None)
        if section is None:
            return True
        return all(c.ok for c in section.checks)

    @property
    def has_lob_peril_conflict(self) -> bool:
        """The one-analysis-per-LOB/peril check found an actual conflict."""
        section = next((s for s in self.sections if s.title == "lob_peril_validation"), None)
        if section is None:
            return False
        return any((not c.ok) and "validation failed" in c.note for c in section.checks)

    @property
    def all_blending_weights_ok(self) -> bool:
        section = next((s for s in self.sections if s.title == "blending_weights_validation"), None)
        if section is None:
            return True
        return all(c.ok for c in section.checks)

    @property
    def has_blending_weights_conflict(self) -> bool:
        section = next((s for s in self.sections if s.title == "blending_weights_validation"), None)
        if section is None:
            return False
        return any(not c.ok for c in section.checks)


def _check_seed(seeds_dir: Path, spec: SeedSpec, *, selected_analyses_exists: bool = False) -> Check:
    """Verify a seed: file exists, column headers match, count rows.

    A seed in `REQUIRED_SEEDS` with zero rows is reported as `ok=False` —
    the pipeline would silently produce zero-row Hisco parquets otherwise.
    Non-required seeds (e.g. `air_events`) may
    legitimately be empty stubs and are reported `ok=True` with `(stub)`.
    """
    if spec.name == "valid_analyses" and selected_analyses_exists:
        path = seeds_dir / spec.filename if spec.filename else seeds_dir / "business" / "valid_analyses.csv"
        return Check(
            label=spec.name,
            path=path,
            ok=True,
            note="compatibility seed ignored because selected_analyses.csv is authoritative",
        )
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


def _check_ylt_parquet(
    path: Path,
    expected_schema: pl.Schema,
    name: str,
    *,
    value_column: str,
) -> Check:
    """Check one YLT parquet: schema, row count, and loss-column sum."""
    try:
        lf = pl.scan_parquet(path)
        actual = lf.collect_schema()
    except Exception as e:
        return Check(label=name, path=path, ok=False, note=f"parse error: {e}")
    size_mb = path.stat().st_size / 1e6
    diffs = column_diff(actual, expected_schema)
    blocking_diffs = [d for d in diffs if d.kind != "unexpected"]
    if not blocking_diffs:
        try:
            stats = lf.select(
                pl.len().alias("rows"),
                pl.col(value_column).sum().fill_null(0.0).alias("value_sum"),
            ).collect()
        except Exception as e:
            return Check(label=name, path=path, ok=False, note=f"{size_mb:.1f} MB | stats error: {e}")

        rows = int(stats["rows"][0])
        value_sum = float(stats["value_sum"][0])
        return Check(
            label=name,
            path=path,
            ok=True,
            rows=rows,
            note=f"{size_mb:.1f} MB | schema OK | {value_column} sum={value_sum:,.2f}",
        )
    return Check(label=name, path=path, ok=False, note=f"{size_mb:.1f} MB | {_format_diffs(diffs)}")


def _check_dir_glob(
    label: str,
    directory: Path,
    glob: str,
    parquet_schema: pl.Schema | None = None,
    parquet_value_column: str | None = None,
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
            if parquet_value_column is None:
                raise ValueError("parquet_value_column is required when parquet_schema is provided")
            checks.append(_check_ylt_parquet(path, parquet_schema, path.name, value_column=parquet_value_column))
        else:
            checks.append(Check(label=path.name, path=path, ok=True, note=f"{path.stat().st_size / 1e6:.1f} MB"))
    return checks


_EP_TYPES = {"AAL", "AEP", "OEP"}


def _check_ep_summary_csv(path: Path, vendor_name: VendorName | None = None) -> Check:
    """Validate one converted EP-summary CSV uses the canonical analyst schema."""
    try:
        sniff = pl.scan_csv(path).collect_schema().names()
        expected = set(F.CANONICAL_EP_SUMMARY.names())
        missing = expected - set(sniff)
        extra = set(sniff) - expected
        if missing or extra:
            bits = []
            if missing:
                bits.append(f"missing={sorted(missing)}")
            if extra:
                bits.append(f"unexpected={sorted(extra)}")
            return Check(label=path.name, path=path, ok=False, note=", ".join(bits))
        df = pl.read_csv(path, schema=F.CANONICAL_EP_SUMMARY)
    except Exception as e:
        return Check(label=path.name, path=path, ok=False, note=f"parse error: {e}")

    if vendor_name is not None:
        wrong_vendor = df.filter(pl.col(EP.VENDOR) != vendor_name.value)
        if wrong_vendor.height:
            return Check(
                label=path.name,
                path=path,
                ok=False,
                rows=wrong_vendor.height,
                note=f"vendor column must be {vendor_name.value!r} for files in this directory",
            )

    invalid_vendor = df.filter(~pl.col(EP.VENDOR).is_in([v.value for v in VendorName]))
    if invalid_vendor.height:
        return Check(label=path.name, path=path, ok=False, rows=invalid_vendor.height, note="unknown vendor values in EP summary")

    missing_lob = df.filter(pl.col(EP.MODELLED_LOB).is_null() | (pl.col(EP.MODELLED_LOB).str.strip_chars() == ""))
    if missing_lob.height:
        return Check(
            label=path.name,
            path=path,
            ok=False,
            rows=missing_lob.height,
            note="modelled_lob is required for every EP-summary row",
        )
    missing_peril = df.filter(pl.col(EP.MODELLED_PERIL).is_null() | (pl.col(EP.MODELLED_PERIL).str.strip_chars() == ""))
    if missing_peril.height:
        return Check(label=path.name, path=path, ok=False, rows=missing_peril.height, note="modelled_peril is required for every EP-summary row")
    invalid_ep_type = df.filter(~pl.col(EP.EP_TYPE).is_in(_EP_TYPES))
    if invalid_ep_type.height:
        return Check(label=path.name, path=path, ok=False, rows=invalid_ep_type.height, note="ep_type must be one of AAL, AEP, OEP")
    negative_rp = df.filter(pl.col(EP.RETURN_PERIOD) < 0)
    if negative_rp.height:
        return Check(label=path.name, path=path, ok=False, rows=negative_rp.height, note="return_period must be >= 0")
    bad_aal = df.filter((pl.col(EP.EP_TYPE) == "AAL") & (pl.col(EP.RETURN_PERIOD) != 0))
    if bad_aal.height:
        return Check(label=path.name, path=path, ok=False, rows=bad_aal.height, note="AAL rows must have return_period 0")
    return Check(label=path.name, path=path, ok=True, rows=df.height, note="canonical EP summary schema OK")


def _check_ep_summary_files(
    vendor_name: VendorName,
    directory: Path,
    glob: str,
    *,
    optional_missing_ok: bool,
    missing_note: str,
) -> list[Check]:
    """Check EP-summary files and enforce canonical CSVs when present."""
    if not directory.exists():
        if optional_missing_ok:
            return [Check(label=glob, path=directory, ok=True, note="optional; directory does not exist")]
        return [Check(label=glob, path=directory, ok=False, note="directory does not exist")]
    files = sorted(directory.glob(glob))
    if not files:
        if optional_missing_ok:
            return [Check(label=glob, path=directory, ok=True, note="optional; use ep-summary-to-csv to generate long CSVs for review")]
        return [Check(label=glob, path=directory, ok=False, note=missing_note)]

    checks: list[Check] = []
    for path in files:
        if path.suffix == ".csv":
            checks.append(_check_ep_summary_csv(path, vendor_name))
        else:
            checks.append(Check(label=path.name, path=path, ok=True, note=f"{path.stat().st_size / 1e6:.1f} MB"))
    return checks


def _empty_ep_summary() -> pl.DataFrame:
    return pl.DataFrame(schema=F.CANONICAL_EP_SUMMARY)


def _collect_ep_summaries(config: Config) -> tuple[pl.DataFrame, list[Check]]:
    """Read all canonical converted EP summaries, returning schema checks."""
    frames: list[pl.DataFrame] = []
    checks: list[Check] = []
    for vendor in config.vendors:
        if not vendor.ep_summary_dir.exists():
            continue
        for path in sorted(vendor.ep_summary_dir.glob("*.long.csv")):
            check = _check_ep_summary_csv(path, vendor.name)
            if check.ok:
                frames.append(pl.read_csv(path, schema=F.CANONICAL_EP_SUMMARY))
            else:
                checks.append(check)
    if not frames:
        return _empty_ep_summary(), checks
    return pl.concat(frames, how="vertical"), checks


def _csv_check(label: str, path: Path, schema: pl.Schema) -> pl.DataFrame | Check:
    try:
        return pl.read_csv(path, schema=schema)
    except Exception as e:
        return Check(label=label, path=path, ok=False, note=f"parse error: {e}")


def _enabled_selected_analyses(path: Path) -> pl.DataFrame | Check:
    selected = _csv_check("selected_analyses", path, F.SELECTED_ANALYSES)
    if isinstance(selected, Check):
        return selected
    return selected.filter(pl.col(SA.INCLUDE))


def _selected_seed_checks(selected: pl.DataFrame, path: Path) -> tuple[pl.DataFrame, list[Check]]:
    """Return enabled selected rows plus validation checks for cheap seed mistakes."""
    checks: list[Check] = []
    enabled = selected.filter(pl.col(SA.INCLUDE))
    blank = enabled.filter(
        pl.col(SA.VENDOR).is_null()
        | (pl.col(SA.VENDOR).str.strip_chars() == "")
        | pl.col(SA.ANALYSIS_ID).is_null()
        | (pl.col(SA.ANALYSIS_ID).str.strip_chars() == "")
    )
    checks.append(Check(
        label="selected analysis IDs non-blank",
        path=path,
        ok=blank.height == 0,
        rows=enabled.height,
        note="all enabled selected analyses have vendor and analysis_id" if blank.height == 0 else f"blank selected rows: {blank.head(10).rows()}",
    ))
    unknown_vendor = enabled.filter(~pl.col(SA.VENDOR).is_in([v.value for v in VendorName]))
    checks.append(Check(
        label="selected analysis vendors",
        path=path,
        ok=unknown_vendor.height == 0,
        rows=enabled.select(SA.VENDOR).unique().height if enabled.height else 0,
        note="all enabled selected vendors are known" if unknown_vendor.height == 0 else f"unknown vendors: {unknown_vendor[SA.VENDOR].unique().to_list()}",
    ))
    duplicates = (
        enabled
        .group_by(SA.VENDOR, SA.ANALYSIS_ID)
        .len()
        .filter(pl.col("len") > 1)
    )
    checks.append(Check(
        label="selected analysis duplicates",
        path=path,
        ok=duplicates.height == 0,
        rows=enabled.height,
        note="no duplicate enabled selected analyses" if duplicates.height == 0 else f"duplicate selected analyses: {duplicates.head(10).rows()}",
    ))
    valid = enabled.filter(
        pl.col(SA.VENDOR).is_in([v.value for v in VendorName])
        & pl.col(SA.VENDOR).is_not_null()
        & (pl.col(SA.VENDOR).str.strip_chars() != "")
        & pl.col(SA.ANALYSIS_ID).is_not_null()
        & (pl.col(SA.ANALYSIS_ID).str.strip_chars() != "")
    ).unique([SA.VENDOR, SA.ANALYSIS_ID])
    return valid, checks


def _selected_valid_analyses_report(selected_meta: pl.DataFrame, valid_path: Path) -> Check:
    """Report legacy valid_analyses divergence while selected_analyses drives runtime."""
    if not valid_path.exists():
        return Check(
            label="valid_analyses compatibility",
            path=valid_path,
            ok=True,
            note="selected_analyses is authoritative; valid_analyses compatibility seed is absent",
        )
    try:
        valid = pl.read_csv(valid_path, schema=F.VALID_ANALYSES)
    except Exception as e:
        return Check(
            label="valid_analyses compatibility",
            path=valid_path,
            ok=True,
            note=f"selected_analyses is authoritative; ignored valid_analyses could not be parsed: {e}",
        )
    selected_runtime = selected_meta.select(
        pl.col(AN.VENDOR),
        pl.col("metadata_analysis_id").alias(AN.ANALYSIS_ID),
    ).unique()
    valid_unique = valid.select(VA.VENDOR, VA.ANALYSIS_ID).unique()
    missing_from_valid = selected_runtime.join(
        valid_unique,
        left_on=[AN.VENDOR, AN.ANALYSIS_ID],
        right_on=[VA.VENDOR, VA.ANALYSIS_ID],
        how="anti",
    )
    extra_valid = valid_unique.join(
        selected_runtime,
        left_on=[VA.VENDOR, VA.ANALYSIS_ID],
        right_on=[AN.VENDOR, AN.ANALYSIS_ID],
        how="anti",
    )
    if missing_from_valid.height == 0 and extra_valid.height == 0:
        note = "valid_analyses matches selected runtime scope; selected_analyses remains authoritative"
    else:
        note = (
            "selected_analyses is authoritative; valid_analyses differs and is ignored "
            f"(missing selected={missing_from_valid.height}, extra valid={extra_valid.height})"
        )
    return Check(
        label="valid_analyses compatibility",
        path=valid_path,
        ok=True,
        rows=valid_unique.height,
        note=note,
    )


def _analysis_ep_key(df: pl.DataFrame, label_col: str) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col(AN.VENDOR) == VendorName.VERISK.value)
        .then(pl.col(label_col))
        .otherwise(pl.col(AN.ANALYSIS_ID))
        .alias("analysis_ep_key"),
    )


def _ep_summary_key(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col(EP.VENDOR) == VendorName.VERISK.value)
        .then(pl.col(EP.MODELLED_PERIL))
        .otherwise(pl.col(EP.ANALYSIS_ID))
        .alias("analysis_ep_key"),
    )


def _check_ylt_selected_coverage(
    config: Config,
    selected_meta: pl.DataFrame,
) -> list[Check]:
    """Verify every enabled selected analysis is present in its vendor YLT input."""
    if selected_meta.height == 0:
        return [Check(label="selected YLT coverage", path=config.seeds_dir, ok=True, note="no selected analyses to check against YLT")]
    checks: list[Check] = []
    risklink_required = selected_meta.filter(pl.col(AN.VENDOR) == VendorName.RISKLINK.value)
    verisk_required = selected_meta.filter(pl.col(AN.VENDOR) == VendorName.VERISK.value)

    if risklink_required.height:
        vendor = config.vendor(VendorName.RISKLINK)
        files = sorted(vendor.ylt_dir.glob(vendor.ylt_glob)) if vendor.ylt_dir.exists() else []
        required = set(risklink_required["selected_analysis_id"].to_list())
        present: set[str] = set()
        try:
            for path in files:
                present.update(
                    pl.scan_parquet(path)
                    .select(pl.col(RLK.ANLS_ID).cast(pl.String).unique())
                    .collect()[RLK.ANLS_ID]
                    .to_list()
                )
        except Exception as e:
            checks.append(Check(label="risklink selected YLT coverage", path=vendor.ylt_dir, ok=False, note=f"parse error: {e}"))
        else:
            missing = sorted(required - present)
            checks.append(Check(
                label="risklink selected YLT coverage",
                path=vendor.ylt_dir,
                ok=not missing,
                rows=len(required),
                note="all selected RiskLink anlsids present" if not missing else f"missing anlsid values: {missing[:10]}",
            ))

    if verisk_required.height:
        vendor = config.vendor(VendorName.VERISK)
        files = sorted(vendor.ylt_dir.glob(vendor.ylt_glob)) if vendor.ylt_dir.exists() else []
        required = set(verisk_required["selected_analysis_id"].to_list())
        present: set[str] = set()
        try:
            for path in files:
                present.update(
                    pl.scan_parquet(path)
                    .select(pl.col(VK.ANALYSIS).unique())
                    .collect()[VK.ANALYSIS]
                    .to_list()
                )
        except Exception as e:
            checks.append(Check(label="verisk selected YLT coverage", path=vendor.ylt_dir, ok=False, note=f"parse error: {e}"))
        else:
            missing = sorted(required - present)
            checks.append(Check(
                label="verisk selected YLT coverage",
                path=vendor.ylt_dir,
                ok=not missing,
                rows=len(required),
                note="all selected Verisk analysis labels present" if not missing else f"missing Analysis labels: {missing[:10]}",
            ))

    if not checks:
        checks.append(Check(label="selected YLT coverage", path=config.seeds_dir, ok=True, note="no selected analyses to check against YLT"))
    return checks


def _check_selected_analyses(config: Config, *, require_ep_summaries: bool) -> list[Check]:
    """Validate analyst-selected analysis IDs against converted EP summaries."""
    seeds_dir = config.seeds_dir
    selected_path = seeds_dir / "business" / "selected_analyses.csv"
    if not selected_path.exists():
        return [Check(
            label="selected_analyses",
            path=selected_path,
            ok=True,
            note="optional; using valid_analyses compatibility path",
        )]

    selected_seed = _csv_check("selected_analyses", selected_path, F.SELECTED_ANALYSES)
    if isinstance(selected_seed, Check):
        return [selected_seed]
    selected, selected_seed_checks = _selected_seed_checks(selected_seed, selected_path)
    checks: list[Check] = [Check(
        label="selected_analyses",
        path=selected_path,
        ok=True,
        rows=selected.height,
        note="enabled analyst-selected analysis IDs",
    )]
    checks.extend(selected_seed_checks)
    if selected.height == 0:
        return checks + [Check(label="selected EP scope", path=selected_path, ok=True, note="no selected analyses enabled")]

    selected_lookup = selected.with_columns(pl.col(SA.ANALYSIS_ID).alias("selected_analysis_id"))
    checks.extend(_check_ylt_selected_coverage(config, selected_lookup))

    lobs_path = seeds_dir / "business" / "lobs.csv"
    analyses_path = seeds_dir / "business" / "analyses.csv"
    perils_path = seeds_dir / "business" / "perils.csv"
    lobs = _csv_check("lobs", lobs_path, F.REF_LOBS)
    analyses = _csv_check("analyses", analyses_path, F.ANALYSES)
    perils = _csv_check("perils", perils_path, F.PERILS)
    parse_errors = [x for x in (lobs, analyses, perils) if isinstance(x, Check)]
    if parse_errors:
        return checks + parse_errors
    assert isinstance(lobs, pl.DataFrame)
    assert isinstance(analyses, pl.DataFrame)
    assert isinstance(perils, pl.DataFrame)

    risklink_selected = selected_lookup.filter(pl.col(SA.VENDOR) == VendorName.RISKLINK.value)
    verisk_selected = selected_lookup.filter(pl.col(SA.VENDOR) == VendorName.VERISK.value)

    missing_risklink_metadata = risklink_selected.join(
        analyses,
        left_on=[SA.VENDOR, "selected_analysis_id"],
        right_on=[AN.VENDOR, AN.ANALYSIS_ID],
        how="anti",
    )
    missing_verisk_metadata = verisk_selected.join(
        analyses,
        left_on=[SA.VENDOR, "selected_analysis_id"],
        right_on=[AN.VENDOR, AN.MODELLED_LABEL],
        how="anti",
    )
    missing_metadata = pl.concat([missing_risklink_metadata, missing_verisk_metadata], how="diagonal")
    checks.append(Check(
        label="selected analysis metadata",
        path=analyses_path,
        ok=missing_metadata.height == 0,
        rows=selected.height,
        note="all selected EP analysis identifiers resolve in analyses.csv" if missing_metadata.height == 0 else f"missing analyses.csv rows: {missing_metadata.select(SA.VENDOR, 'selected_analysis_id').head(10).rows()}",
    ))

    risklink_meta = risklink_selected.join(
        analyses,
        left_on=[SA.VENDOR, "selected_analysis_id"],
        right_on=[AN.VENDOR, AN.ANALYSIS_ID],
        how="inner",
    )
    verisk_meta = verisk_selected.join(
        analyses,
        left_on=[SA.VENDOR, "selected_analysis_id"],
        right_on=[AN.VENDOR, AN.MODELLED_LABEL],
        how="inner",
    )
    raw_meta = pl.concat([risklink_meta, verisk_meta], how="diagonal")
    metadata_analysis_expr = (
        pl.coalesce(pl.col("analysis_id_right"), pl.col(AN.ANALYSIS_ID))
        if "analysis_id_right" in raw_meta.columns
        else pl.col(AN.ANALYSIS_ID)
    )
    selected_meta = raw_meta.with_columns(
        metadata_analysis_expr.alias("metadata_analysis_id")
    ).join(perils, on=AN.PERIL_ID, how="left")
    checks.append(_selected_valid_analyses_report(selected_meta, seeds_dir / "business" / "valid_analyses.csv"))
    missing_peril = selected_meta.filter(pl.col(PR.NAME).is_null())
    checks.append(Check(
        label="selected peril resolution",
        path=perils_path,
        ok=missing_peril.height == 0,
        rows=selected_meta.height,
        note="all selected analyses resolve to canonical peril_id/name/family" if missing_peril.height == 0 else f"missing perils.csv rows: {missing_peril.head(10).rows()}",
    ))

    selected_meta = selected_meta.with_columns(pl.col("selected_analysis_id").alias("analysis_ep_key")).select(
        AN.VENDOR,
        "selected_analysis_id",
        "metadata_analysis_id",
        AN.PERIL_ID,
        PR.NAME,
        PR.PERIL_FAMILY,
        "analysis_ep_key",
    )

    ep, ep_schema_errors = _collect_ep_summaries(config)
    checks.extend(ep_schema_errors)
    if ep.height == 0:
        checks.append(Check(
            label="selected EP summaries",
            path=config.seeds_dir,
            ok=False,
            note="no canonical *.long.csv EP summaries to validate selected analyses against",
        ))
        return checks

    ep_keyed = _ep_summary_key(ep)
    selected_keys = selected_meta.select(AN.VENDOR, "selected_analysis_id", "analysis_ep_key").unique()
    ep_keys = ep_keyed.select(EP.VENDOR, "analysis_ep_key").unique()
    missing_ep = selected_keys.join(
        ep_keys,
        left_on=[AN.VENDOR, "analysis_ep_key"],
        right_on=[EP.VENDOR, "analysis_ep_key"],
        how="anti",
    )
    checks.append(Check(
        label="selected analysis IDs in EP summary",
        path=selected_path,
        ok=missing_ep.height == 0,
        rows=selected_keys.height,
        note="all selected analysis IDs exist in converted EP summaries" if missing_ep.height == 0 else f"missing from EP summaries: {missing_ep.head(10).rows()}",
    ))

    selected_ep_rows = ep_keyed.join(
        selected_meta,
        left_on=[EP.VENDOR, "analysis_ep_key"],
        right_on=[AN.VENDOR, "analysis_ep_key"],
        how="inner",
    )
    unknown_lobs = selected_ep_rows.select(EP.MODELLED_LOB).unique().join(
        lobs.select(LB.MODELLED_LOB).unique(),
        left_on=EP.MODELLED_LOB,
        right_on=LB.MODELLED_LOB,
        how="anti",
    )
    checks.append(Check(
        label="selected modelled_lob mapping",
        path=lobs_path,
        ok=unknown_lobs.height == 0,
        rows=selected_ep_rows.select(EP.MODELLED_LOB).unique().height if selected_ep_rows.height else 0,
        note="all selected EP modelled_lob values resolve to lobs.csv" if unknown_lobs.height == 0 else f"unknown modelled_lob values: {unknown_lobs[EP.MODELLED_LOB].head(10).to_list()}",
    ))

    resolved_scope = selected_ep_rows.join(
        lobs,
        left_on=EP.MODELLED_LOB,
        right_on=LB.MODELLED_LOB,
        how="inner",
    ).select(
        EP.VENDOR,
        "selected_analysis_id",
        "metadata_analysis_id",
        EP.MODELLED_LOB,
        LB.LOB_ID,
        LB.ROLLUP_LOB,
        LB.OFFICE,
        LB.CLASS,
        LB.CURRENCY,
        AN.PERIL_ID,
        PR.NAME,
        PR.PERIL_FAMILY,
    ).unique()
    checks.append(Check(
        label="selected EP scope",
        path=selected_path,
        ok=resolved_scope.height > 0 and all(c.ok for c in checks[-4:]),
        rows=resolved_scope.height,
        note="resolved selected analyses to lob_id/rollup_lob and canonical peril" if resolved_scope.height > 0 else "no selected EP rows resolved to scope",
    ))
    return checks


def _check_forecast_coverage(seeds_dir: Path) -> list[Check]:
    """Report forecast dates and coverage for scoped LOB/analysis rows.

    Forecast factors join at runtime on ``(office, class)``. Missing rows are
    currently filled as factor 1.0, so the plan surfaces coverage gaps before
    the operator starts a run.
    """
    lobs_path = seeds_dir / "business" / "lobs.csv"
    ff_path = seeds_dir / "vor" / "forecast_factors.csv"
    if not lobs_path.exists() or not ff_path.exists():
        return [Check(label="forecast coverage", path=seeds_dir, ok=False, note="missing seed input")]

    try:
        lobs = pl.read_csv(lobs_path, schema=F.REF_LOBS)
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


def _check_one_analysis_per_lob_peril(seeds_dir: Path) -> list[Check]:
    """Validate the allow-list selects one analysis per LOB/peril key.

    RiskLink analysis metadata carries ``lob_id``, so the key is
    ``(vendor, lob_id, peril_id)``. Verisk analysis metadata is peril-level and
    the LOB arrives on the YLT row, so the key is ``(vendor, peril_id)``. This
    catches ambiguous operator selections such as both default and adjusted
    analyses for the same modelling bucket.
    """
    lobs_path = seeds_dir / "business" / "lobs.csv"
    analyses_path = seeds_dir / "business" / "analyses.csv"
    valid_path = seeds_dir / "business" / "valid_analyses.csv"
    selected_path = selected_analyses_path(seeds_dir)
    if not lobs_path.exists() or not analyses_path.exists() or (not valid_path.exists() and not selected_path.exists()):
        return [Check(label="one analysis per lob/peril", path=seeds_dir, ok=False, note="missing seed input")]

    try:
        lobs = pl.read_csv(lobs_path, schema=F.REF_LOBS)
        analyses = pl.read_csv(analyses_path, schema=F.ANALYSES)
        valid_analyses = (
            pl.read_csv(valid_path, schema=F.VALID_ANALYSES)
            if valid_path.exists()
            else pl.DataFrame(schema=F.VALID_ANALYSES)
        )
    except Exception as e:
        return [Check(label="one analysis per lob/peril", path=seeds_dir, ok=False, note=f"parse error: {e}")]

    try:
        effective = analyses_with_selected_ids_for_run(
            seeds_dir,
            analyses.lazy(),
            valid_analyses.lazy(),
            validate_selected=True,
        ).collect()
    except Exception as e:
        return [Check(label="one analysis per lob/peril", path=selected_path, ok=False, note=f"selected analysis resolution failed: {e}")]

    scope_path = selected_path if selected_path.exists() else valid_path

    mapped = (
        effective
        .join(lobs.select(LB.LOB_ID, LB.MODELLED_LOB), on=AN.LOB_ID, how="left")
        .with_columns(
            pl.when(pl.col(AN.LOB_ID).is_null())
            .then(pl.lit("<all Verisk LOBs>"))
            .otherwise(pl.col(LB.MODELLED_LOB))
            .alias("lob_key"),
        )
    )
    if mapped.height == 0:
        return [Check(
            label="one analysis per lob/peril",
            path=scope_path,
            ok=True,
            note="no valid analyses to validate",
        )]

    conflicts = (
        mapped
        .group_by(AN.VENDOR, "lob_key", AN.PERIL_ID)
        .agg(
            pl.col(AN.ANALYSIS_ID).sort().alias("analysis_ids"),
            pl.col(AN.MODELLED_LABEL).sort().alias("modelled_labels"),
            pl.len().alias("n_analyses"),
        )
        .filter(pl.col("n_analyses") > 1)
    )
    if conflicts.height == 0:
        return [Check(
            label="one analysis per lob/peril",
            path=scope_path,
            ok=True,
            rows=mapped.select(AN.VENDOR, "lob_key", AN.PERIL_ID).unique().height,
            note="effective analysis scope selects at most one analysis per lob/peril",
        )]

    examples = "; ".join(
        f"{row[AN.VENDOR]} {row['lob_key']} peril={row[AN.PERIL_ID]} -> {row['analysis_ids']}"
        for row in conflicts.head(5).iter_rows(named=True)
    )
    return [Check(
        label="one analysis per lob/peril",
        path=scope_path,
        ok=False,
        rows=conflicts.height,
        note=f"one analysis per lob/peril validation failed; examples: {examples}",
    )]


def _check_blending_weight_keys(seeds_dir: Path) -> list[Check]:
    """Validate blending weight rows can join to runtime peril/sub-peril keys."""
    bw_path = seeds_dir / "vor" / "blending_weights.csv"
    analyses_path = seeds_dir / "business" / "analyses.csv"
    perils_path = seeds_dir / "business" / "perils.csv"
    if not bw_path.exists() or not analyses_path.exists() or not perils_path.exists():
        return [Check(label="blending weight keys", path=seeds_dir, ok=True, note="not checked; missing seed input")]
    try:
        bw = pl.read_csv(bw_path, schema=F.BLENDING_WEIGHTS)
        analyses = pl.read_csv(analyses_path, schema=F.ANALYSES)
        perils = pl.read_csv(perils_path, schema=F.PERILS)
    except Exception as e:
        return [Check(label="blending weight keys", path=bw_path, ok=False, note=f"parse error: {e}")]

    unknown_perils = bw.select(BW.PERIL_ID).unique().join(
        perils.select(PR.PERIL_ID).unique(),
        left_on=BW.PERIL_ID,
        right_on=PR.PERIL_ID,
        how="anti",
    )
    sub_rows = bw.filter(pl.col(BW.SUB_PERIL).is_not_null() & (pl.col(BW.SUB_PERIL).str.strip_chars() != ""))
    valid_sub_keys = analyses.select(AN.PERIL_ID, AN.MODELLED_LABEL).unique()
    unmatched_sub = sub_rows.select(BW.PERIL_ID, BW.SUB_PERIL).unique().join(
        valid_sub_keys,
        left_on=[BW.PERIL_ID, BW.SUB_PERIL],
        right_on=[AN.PERIL_ID, AN.MODELLED_LABEL],
        how="anti",
    )

    checks = [Check(
        label="blending peril_id keys",
        path=bw_path,
        ok=unknown_perils.height == 0,
        rows=bw.select(BW.PERIL_ID).unique().height if bw.height else 0,
        note="all blending peril_id values exist in perils.csv" if unknown_perils.height == 0 else f"unknown peril_id values: {unknown_perils[BW.PERIL_ID].head(10).to_list()}",
    )]
    checks.append(Check(
        label="blending sub_peril keys",
        path=bw_path,
        ok=unmatched_sub.height == 0,
        rows=sub_rows.select(BW.PERIL_ID, BW.SUB_PERIL).unique().height if sub_rows.height else 0,
        note="all non-empty sub_peril values match analyses.modelled_label for the same peril_id" if unmatched_sub.height == 0 else f"unmatched sub_peril keys: {unmatched_sub.head(10).rows()}",
    ))
    return checks


def build_plan(config: Config, *, require_ep_summaries: bool = False) -> Plan:
    sections: list[Section] = []

    seed_specs = discover_seeds(config.seeds_dir)
    selected_exists = has_selected_analyses_seed(config.seeds_dir)
    sections.append(Section(
        title="seeds",
        header=str(config.seeds_dir),
        checks=[_check_seed(config.seeds_dir, spec, selected_analyses_exists=selected_exists) for spec in seed_specs],
    ))

    ylt_schemas: dict[VendorName, pl.Schema] = {
        VendorName.VERISK: F.RAW_VERISK_YLT,
        VendorName.RISKLINK: F.RAW_RISKLINK_YLT,
    }
    ylt_value_columns: dict[VendorName, str] = {
        VendorName.VERISK: VK.NET_PRE_CAT_LOSS,
        VendorName.RISKLINK: RLK.LOSS,
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
                parquet_value_column=ylt_value_columns.get(vendor.name),
            ),
        ))

    for vendor in config.vendors:
        ep_glob = "*.long.csv" if require_ep_summaries else vendor.ep_summary_glob
        sections.append(Section(
            title=f"ep_summaries {vendor.name}",
            header=f"{vendor.ep_summary_dir}  pattern={ep_glob}",
            checks=_check_ep_summary_files(
                vendor.name,
                vendor.ep_summary_dir,
                ep_glob,
                optional_missing_ok=not require_ep_summaries,
                missing_note="optional; use ep-summary-to-csv to generate long CSVs for review",
            ),
        ))

    sections.append(Section(
        title="selected_analysis_validation",
        header=str(config.seeds_dir / "business" / "selected_analyses.csv"),
        checks=_check_selected_analyses(config, require_ep_summaries=require_ep_summaries),
    ))

    sections.append(Section(
        title="lob_peril_validation",
        header=str(selected_analyses_path(config.seeds_dir) if selected_exists else config.seeds_dir / "business" / "valid_analyses.csv"),
        checks=_check_one_analysis_per_lob_peril(config.seeds_dir),
    ))

    sections.append(Section(
        title="blending_weights_validation",
        header=str(config.seeds_dir / "vor" / "blending_weights.csv"),
        checks=_check_blending_weight_keys(config.seeds_dir),
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
