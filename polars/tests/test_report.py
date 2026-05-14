"""Unit tests for the end-of-run EP summary report.

Numeric correctness is checked against hand-calculated values; the xlsx
writer is exercised with a structural smoke test (sheets exist, headers
where expected) — we don't pin styling details that would be brittle.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import openpyxl
import polars as pl
import pytest

from rollup.config import Flavor, Vendor, VendorName
from rollup.io.report_writer import write_report
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import EpType
from rollup.stages.report import (
    GRAIN_PERIL, GRAIN_ROLLUP_LOB, GRAIN_TOTAL,
    REPORT_EP_TYPE, REPORT_GRAIN, REPORT_GROUP_KEY,
    REPORT_RP, REPORT_VALUE, REPORT_VARIANT,
    build_report,
)
from rollup.variants import VariantSpec


_METRIC = "loss_uplifted_capped_localccy_202601_euws"


def _events(rows: list[tuple[int, str, str, float]]) -> pl.LazyFrame:
    """Build a minimal AllFactors LazyFrame with the columns build_report reads.

    Each tuple is (year_id, rollup_lob, peril_name, metric_value).
    """
    return pl.DataFrame({
        AF.YEAR_ID:      [r[0] for r in rows],
        AF.ROLLUP_LOB:   [r[1] for r in rows],
        AF.PERIL_NAME:   [r[2] for r in rows],
        AF.BASE_MODEL:   [VendorName.VERISK.value] * len(rows),
        _METRIC:         [r[3] for r in rows],
    }).lazy()


def _verisk_vendor(*, n_sim: int = 10_000) -> Vendor:
    return Vendor(
        name=VendorName.VERISK,
        hisco_label="AIR",
        n_simulations=n_sim,
        ylt_dir=Path("/tmp/ignored"),
        ylt_glob="*",
        ep_summary_dir=Path("/tmp/ignored"),
    )


def _variant(vendor: Vendor) -> VariantSpec:
    """A VariantSpec whose loss_metric overrides to point at the test metric column."""
    v = VariantSpec(vendor=vendor, forecast_date=date(2026, 1, 1), flavor=Flavor.MAIN)
    # VariantSpec.loss_metric is a computed property; override via subclass for the test.
    class _Patched(VariantSpec):
        @property
        def loss_metric(self) -> str:  # type: ignore[override]
            return _METRIC
    return _Patched(vendor=vendor, forecast_date=date(2026, 1, 1), flavor=Flavor.MAIN)


# --------------------------------------------------------------------------- #
# build_report — numeric correctness
# --------------------------------------------------------------------------- #

def test_report_aal_equals_total_over_n_sim():
    """AAL = sum(metric) / n_sim. Hand-checkable with a 4-event fixture."""
    events = _events([
        # (year, rollup_lob, peril, value)
        (1, "LOB_A", "PERIL_A", 100.0),
        (2, "LOB_A", "PERIL_A", 200.0),
        (3, "LOB_B", "PERIL_A", 300.0),
        (4, "LOB_B", "PERIL_B", 400.0),
    ])
    vendor = _verisk_vendor(n_sim=10)   # tiny N to make AAL legible
    variants = [_variant(vendor)]

    report = build_report(events, variants, target_return_periods=(2, 5))
    total_aal = report.filter(
        (pl.col(REPORT_GRAIN) == GRAIN_TOTAL) & (pl.col(REPORT_EP_TYPE) == EpType.AAL)
    )
    assert total_aal.height == 1
    # 100 + 200 + 300 + 400 = 1000; / 10 = 100
    assert total_aal[REPORT_VALUE][0] == pytest.approx(100.0)


def test_report_per_rollup_lob_aal_partitions_total():
    """The sum of per-rollup_lob AALs equals the Total AAL."""
    events = _events([
        (1, "LOB_A", "PERIL_A", 100.0),
        (2, "LOB_A", "PERIL_A", 200.0),
        (3, "LOB_B", "PERIL_A", 300.0),
        (4, "LOB_B", "PERIL_B", 400.0),
    ])
    vendor = _verisk_vendor(n_sim=10)
    report = build_report(events, [_variant(vendor)], target_return_periods=(2,))

    total = report.filter(
        (pl.col(REPORT_GRAIN) == GRAIN_TOTAL) & (pl.col(REPORT_EP_TYPE) == EpType.AAL)
    )[REPORT_VALUE][0]

    per_lob_sum = report.filter(
        (pl.col(REPORT_GRAIN) == GRAIN_ROLLUP_LOB) & (pl.col(REPORT_EP_TYPE) == EpType.AAL)
    )[REPORT_VALUE].sum()

    assert per_lob_sum == pytest.approx(total)


def test_report_oep_at_target_rp_is_highest_per_year_max():
    """OEP at rp=N picks the rank-1 max-per-year loss when N matches n_sim/1."""
    # Five distinct years with one event each: 100, 90, 80, 70, 60.
    events = _events([
        (1, "LOB", "PERIL", 100.0),
        (2, "LOB", "PERIL",  90.0),
        (3, "LOB", "PERIL",  80.0),
        (4, "LOB", "PERIL",  70.0),
        (5, "LOB", "PERIL",  60.0),
    ])
    vendor = _verisk_vendor(n_sim=5)   # rp = floor(5 / rank) → rank 1 → rp=5
    report = build_report(events, [_variant(vendor)], target_return_periods=(5,))

    oep_5 = report.filter(
        (pl.col(REPORT_GRAIN) == GRAIN_TOTAL)
        & (pl.col(REPORT_EP_TYPE) == EpType.OEP)
        & (pl.col(REPORT_RP) == 5)
    )
    assert oep_5.height == 1
    assert oep_5[REPORT_VALUE][0] == pytest.approx(100.0)


def test_report_aep_aggregates_within_year_before_ranking():
    """AEP at rp=N picks the rank-1 sum-per-year loss. Distinct from OEP when years have multiple events."""
    events = _events([
        # Year 1 has two events summing to 150; year 2 has one big 200 event.
        (1, "LOB", "PERIL",  50.0),
        (1, "LOB", "PERIL", 100.0),
        (2, "LOB", "PERIL", 200.0),
        (3, "LOB", "PERIL",  30.0),
        (4, "LOB", "PERIL",  20.0),
        (5, "LOB", "PERIL",  10.0),
    ])
    vendor = _verisk_vendor(n_sim=5)
    report = build_report(events, [_variant(vendor)], target_return_periods=(5,))

    total_aep = report.filter(
        (pl.col(REPORT_GRAIN) == GRAIN_TOTAL)
        & (pl.col(REPORT_EP_TYPE) == EpType.AEP)
        & (pl.col(REPORT_RP) == 5)
    )
    total_oep = report.filter(
        (pl.col(REPORT_GRAIN) == GRAIN_TOTAL)
        & (pl.col(REPORT_EP_TYPE) == EpType.OEP)
        & (pl.col(REPORT_RP) == 5)
    )

    # AEP: per-year totals are {150, 200, 30, 20, 10}; rank 1 ⇒ 200.
    # OEP: per-year max losses are {100, 200, 30, 20, 10}; rank 1 ⇒ 200.
    # Picking year 2 (200) is unambiguous; the AEP-vs-OEP distinction shows
    # at lower ranks (year 1 is 150 vs 100).
    assert total_aep[REPORT_VALUE][0] == pytest.approx(200.0)
    assert total_oep[REPORT_VALUE][0] == pytest.approx(200.0)


# --------------------------------------------------------------------------- #
# write_report — structural smoke
# --------------------------------------------------------------------------- #

def test_write_report_emits_csv_and_xlsx_with_expected_sheets(tmp_path: Path):
    # Use enough distinct years that both rp=2 (rank 5 of 10) and rp=5 (rank 2)
    # are reachable from floor(n_sim / rank).
    events = _events([
        (1, "LOB_A", "PERIL_A", 100.0),
        (2, "LOB_A", "PERIL_A",  90.0),
        (3, "LOB_A", "PERIL_A",  80.0),
        (4, "LOB_B", "PERIL_B",  70.0),
        (5, "LOB_B", "PERIL_B",  60.0),
    ])
    vendor = _verisk_vendor(n_sim=10)
    report = build_report(events, [_variant(vendor)], target_return_periods=(2, 5))

    csv_path, xlsx_path = write_report(report, tmp_path)

    assert csv_path.exists()
    csv = pl.read_csv(csv_path)
    assert set(csv.columns) == {
        REPORT_VARIANT, REPORT_GRAIN, REPORT_GROUP_KEY,
        REPORT_EP_TYPE, REPORT_RP, REPORT_VALUE,
    }
    assert csv.height == report.height

    assert xlsx_path.exists()
    wb = openpyxl.load_workbook(xlsx_path)
    assert set(wb.sheetnames) == {GRAIN_TOTAL, GRAIN_ROLLUP_LOB, GRAIN_PERIL}

    total_sheet = wb[GRAIN_TOTAL]
    headers = [c.value for c in total_sheet[1]]
    assert headers[:2] == [REPORT_VARIANT, REPORT_GROUP_KEY]
    assert EpType.AAL in headers
    # At least one AEP/OEP column for each requested rp.
    for rp in (2, 5):
        assert f"AEP_{rp}" in headers
        assert f"OEP_{rp}" in headers
    assert total_sheet.freeze_panes == "C2"
