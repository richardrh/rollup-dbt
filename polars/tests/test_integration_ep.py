"""End-to-end EP curve run against the real AIR YLT parquets.

Gated on the files existing under `data/ylt/verisk/` (resolved from
`config.resolve()` so `ROLLUP_YLT_VERISK_DIR` overrides are honoured).
Skips silently if the parquets aren't checked out, so the rest of the
suite stays cheap.

What this test does:
  1. Glob-loads every `air_ylt_*.parquet` in the configured Verisk YLT dir.
  2. Filters to `trim(upper(CatalogTypeCode)) LIKE '%STC%'`.
  3. Projects into `NormalizedYlt` shape — without real dim tables, lob_id /
     region_peril_id are dense-ranked ints per unique (ExposureAttribute,
     Analysis); labels are preserved for human inspection.
  4. Runs `ep_curve_from_ylt` at the granularity duckdb produced — one curve
     per (vendor, lob, region_peril), plus an "overall" roll-up for quick
     sanity against excel totals.
  5. Writes CSV outputs to `polars/tests/outputs/`.

The CSVs in `tests/outputs/` are what you diff against the reference RMS
spreadsheets in `data/ep_summaries/risklink/`.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from rollup import config
from rollup.config import VendorName
from rollup.schemas import frames as F
from rollup.schemas.columns import EpCurveCol as EP
from rollup.schemas.columns import EpType
from rollup.schemas.columns import NormalizedYltCol as Y
from rollup.schemas.columns import RawVeriskYltCol as VK
from rollup.stages.ep import ep_curve_from_ylt
from rollup.staging import load_raw_verisk_ylt


OUTPUTS_DIR = Path(__file__).resolve().parent / "outputs"

VERISK = next(v for v in config.resolve().vendors if v.name == VendorName.VERISK)


def _verisk_parquets() -> list[Path]:
    return sorted(VERISK.ylt_dir.glob(VERISK.ylt_glob))


pytestmark = pytest.mark.skipif(
    not _verisk_parquets(),
    reason=f"{VERISK.ylt_dir}/{VERISK.ylt_glob} not present — integration test skipped",
)


# -----------------------------------------------------------------------------
# Projection: raw AIR wire schema → NormalizedYlt shape
# -----------------------------------------------------------------------------

def _project_to_normalized(raw: pl.LazyFrame) -> pl.LazyFrame:
    """Synthesize NormalizedYlt from the raw Verisk parquet.

    In the full pipeline `normalize_verisk_ylt` would join dim_region_perils +
    reference.lobs to produce real ids + rollup labels. Until those seeds
    are populated from the duckdb export, we dense-rank the string keys so
    the shape passes validate_schema and the groupby inside ep.py still
    partitions correctly.
    """
    return (
        raw
        .filter(pl.col(VK.CATALOG_TYPE_CODE).str.strip_chars().str.to_uppercase().str.contains("STC"))
        .with_columns(
            pl.col(VK.EXPOSURE_ATTRIBUTE).rank("dense").cast(pl.Int64).alias(Y.LOB_ID),
            pl.col(VK.ANALYSIS).rank("dense").cast(pl.Int64).alias(Y.REGION_PERIL_ID),
        )
        .select(
            pl.lit(VendorName.VERISK).alias(Y.VENDOR),
            pl.col(Y.LOB_ID),
            pl.col(VK.EXPOSURE_ATTRIBUTE).alias(Y.MODELLED_LOB),
            pl.col(VK.EXPOSURE_ATTRIBUTE).alias(Y.ROLLUP_LOB),         # passthrough until seeds wired
            pl.lit("prop").alias(Y.LOB_TYPE),                           # placeholder
            pl.col(VK.EXPOSURE_ATTRIBUTE).alias(Y.CDS_CAT_CLASS_NAME),  # placeholder
            pl.lit("UK").alias(Y.OFFICE),                               # placeholder
            pl.lit("HH").alias(Y.LOB_CLASS),                            # placeholder
            pl.col(Y.REGION_PERIL_ID),
            pl.col(VK.ANALYSIS).alias(Y.MODELLED_REGION_PERIL),
            pl.col(VK.ANALYSIS).alias(Y.PERIL_NAME),                    # placeholder
            pl.lit("EU").alias(Y.REGION),                               # placeholder
            pl.lit("WS").alias(Y.PERIL_FAMILY),                         # placeholder
            pl.lit("GBP").alias(Y.CURRENCY),                            # placeholder
            pl.col(VK.MODEL_CODE).alias(Y.MODEL_CODE),
            pl.col(VK.YEAR_ID).alias(Y.YEAR_ID),
            pl.col(VK.EVENT_ID).alias(Y.EVENT_ID),
            pl.col(VK.NET_PRE_CAT_LOSS).alias(Y.LOSS),
        )
    )


# -----------------------------------------------------------------------------
# Loads + projects the YLT once per session — 4.4M rows, don't repeat.
# -----------------------------------------------------------------------------

@pytest.fixture(scope="module")
def normalized_ylt() -> pl.LazyFrame:
    raw = load_raw_verisk_ylt(VERISK.ylt_dir, glob=VERISK.ylt_glob)
    return _project_to_normalized(raw)


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------

def test_verisk_parquet_wire_schema_matches(normalized_ylt):
    """Sanity: the parquet still has the columns we built the VK enum against."""
    schema = pl.scan_parquet(str(VERISK.ylt_dir / VERISK.ylt_glob)).collect_schema()
    expected = set(F.RAW_VERISK_YLT.names())
    missing = expected - set(schema.names())
    assert not missing, f"raw Verisk parquet missing expected columns: {missing}"


def test_normalized_projection_matches_schema(normalized_ylt):
    head = normalized_ylt.head(10).collect()
    assert head.schema == F.NORMALIZED_YLT
    assert head.height == 10


def test_ep_curve_runs_and_writes_csv(normalized_ylt):
    """Full ep_curve_from_ylt on real 4.4M-row YLT; write CSV for manual diff vs excel."""
    ep = ep_curve_from_ylt(normalized_ylt, n_simulations=VERISK.n_simulations)

    # Materialize once, with stable sort so diffs against excel are reproducible.
    df = (
        ep.collect()
        .sort(by=[EP.ROLLUP_LOB, EP.PERIL_NAME, EP.EP_TYPE, EP.RANK_NUM])
    )

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUTS_DIR / "air_ep_curve_by_lob_peril.csv"
    df.write_csv(out_path)

    assert df.schema == F.EP_CURVE
    assert df.height > 0
    # Every (lob, peril) pair should have at least an AAL row and some AEP/OEP rows.
    by_type = df.group_by(EP.EP_TYPE).agg(pl.len().alias("n"))
    types_present = {row[EP.EP_TYPE] for row in by_type.iter_rows(named=True)}
    assert types_present == {EpType.AAL, EpType.AEP, EpType.OEP}, f"missing ep_type variants: {types_present}"

    print(f"\n[integration] wrote {df.height:,} rows to {out_path}")


def test_overall_ep_curve_for_excel_diff(normalized_ylt):
    """Collapse every YLT row into a single (vendor, lob, peril) key so we get
    one overall EP curve — the closest comparable to the excel totals sheet."""
    overall_ylt = (
        normalized_ylt
        .with_columns(
            pl.lit(0, dtype=pl.Int64).alias(Y.LOB_ID),
            pl.lit(0, dtype=pl.Int64).alias(Y.REGION_PERIL_ID),
            pl.lit("ALL").alias(Y.MODELLED_LOB),
            pl.lit("ALL").alias(Y.ROLLUP_LOB),
            pl.lit("ALL").alias(Y.MODELLED_REGION_PERIL),
            pl.lit("ALL").alias(Y.PERIL_NAME),
            pl.lit("ALL").alias(Y.REGION),
            pl.lit("ALL").alias(Y.PERIL_FAMILY),
            pl.lit("ALL").alias(Y.CDS_CAT_CLASS_NAME),
        )
    )

    ep = ep_curve_from_ylt(overall_ylt, n_simulations=VERISK.n_simulations)
    df = ep.collect().sort(by=[EP.EP_TYPE, EP.RANK_NUM])

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUTS_DIR / "air_ep_curve_overall.csv"
    df.write_csv(out_path)

    aal_row = df.filter(pl.col(EP.EP_TYPE) == EpType.AAL).row(0, named=True)
    aal_from_curve = aal_row[EP.LOSS]

    # Independent sanity: AAL from the curve must equal sum(loss)/n_sims.
    total_loss = normalized_ylt.select(pl.col(Y.LOSS).sum()).collect().item()
    expected_aal = total_loss / VERISK.n_simulations
    assert aal_from_curve == pytest.approx(expected_aal, rel=1e-9), (
        f"overall AAL mismatch: curve={aal_from_curve:.2f}, direct={expected_aal:.2f}"
    )

    print(f"\n[integration] overall AAL = {aal_from_curve:,.2f} "
          f"(total_loss = {total_loss:,.2f} / {VERISK.n_simulations})")
    print(f"[integration] wrote overall EP curve ({df.height} rows) to {out_path}")


def test_aal_per_modelled_lob_summary(normalized_ylt):
    """Per-lob AAL snapshot — one row per ExposureAttribute, sum(loss)/n_sims.
    Easy to eyeball-compare against the "RMS by LOB" sheet."""
    df = (
        normalized_ylt
        .group_by(Y.MODELLED_LOB)
        .agg((pl.col(Y.LOSS).sum() / VERISK.n_simulations).alias("aal"))
        .sort("aal", descending=True)
    ).collect()

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUTS_DIR / "air_aal_by_lob.csv"
    df.write_csv(out_path)

    assert df.height > 0
    print(f"\n[integration] wrote per-lob AAL ({df.height} lobs) to {out_path}")
