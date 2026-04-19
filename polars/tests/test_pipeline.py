"""Pipeline scaffolding: VariantSpec + build_variants + seed-driven forecast dates."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from rollup import config
from rollup.config import Flavor, Vendor, VendorName
from rollup.pipeline import (
    VariantSpec,
    _compute_dialsup,
    build_variants,
    count_event_id_orphans,
    forecast_dates_from_seed,
)
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import MetricCol as M
from rollup.schemas.columns import NormalizedYltCol as Y
from rollup.schemas.columns import RefAirEventsCol as AE
from rollup.seeds import load_all


SEEDS_DIR = Path(__file__).resolve().parent.parent / "seeds"


# -----------------------------------------------------------------------------
# VariantSpec behaves like a typed triple, not a raw tuple
# -----------------------------------------------------------------------------

def _fake_vendor(name: VendorName, label: str, flavors=None) -> Vendor:
    return Vendor(
        name=name,
        hisco_label=label,
        n_simulations=10_000,
        ylt_dir=Path("/tmp"),
        ylt_glob="*.parquet",
        ep_summary_dir=Path("/tmp"),
        flavors=flavors or (Flavor.MAIN, Flavor.DIALSUP),
    )


def test_variant_name_follows_hisco_convention():
    v = _fake_vendor(VendorName.VERISK, "AIR")
    spec = VariantSpec(vendor=v, forecast_date=date(2026, 1, 1), flavor=Flavor.MAIN)
    assert spec.name == "HiscoAIR_202601_main"


def test_variant_forecast_tag_is_yyyymm():
    v = _fake_vendor(VendorName.RISKLINK, "RMS")
    spec = VariantSpec(vendor=v, forecast_date=date(2027, 7, 1), flavor=Flavor.MAIN)
    assert spec.forecast_tag == "202707"


def test_variant_loss_metric_per_flavor():
    v = _fake_vendor(VendorName.VERISK, "AIR")
    main    = VariantSpec(v, date(2026, 1, 1), Flavor.MAIN).loss_metric
    dialsup = VariantSpec(v, date(2026, 1, 1), Flavor.DIALSUP).loss_metric
    # fa_gross IS in the column name — it's the last factor in the chain, not a flavour.
    assert main    == "loss_uplifted_capped_localccy_202601_euws_fagross"
    assert dialsup == "dialsup_202601"


# -----------------------------------------------------------------------------
# build_variants: cross-product, sorted by date, respects each vendor's flavors
# -----------------------------------------------------------------------------

def test_build_variants_cross_product_count():
    vendors = (_fake_vendor(VendorName.VERISK, "AIR"), _fake_vendor(VendorName.RISKLINK, "RMS"))
    dates = [date(2026, 1, 1), date(2026, 7, 1), date(2027, 1, 1)]
    variants = build_variants(dates, vendors)
    # 2 vendors × 3 dates × 2 flavors
    assert len(variants) == 2 * 3 * 2


def test_build_variants_respects_per_vendor_flavors():
    """A vendor that only emits one flavor should produce only that flavor."""
    slim_vendor = _fake_vendor(VendorName.VERISK, "AIR", flavors=(Flavor.MAIN,))
    variants = build_variants([date(2026, 1, 1), date(2027, 1, 1)], (slim_vendor,))
    assert {v.flavor for v in variants} == {Flavor.MAIN}
    assert len(variants) == 2


def test_build_variants_names_are_all_unique():
    vendors = (_fake_vendor(VendorName.VERISK, "AIR"), _fake_vendor(VendorName.RISKLINK, "RMS"))
    dates = [date(2026, 1, 1), date(2026, 7, 1)]
    variants = build_variants(dates, vendors)
    names = [v.name for v in variants]
    assert len(names) == len(set(names))


def test_build_variants_names_match_hisco_pattern():
    """Every name is HiscoXXX_yyyyMM_<flavor>."""
    vendors = (_fake_vendor(VendorName.VERISK, "AIR"), _fake_vendor(VendorName.RISKLINK, "RMS"))
    variants = build_variants([date(2026, 1, 1)], vendors)
    for v in variants:
        assert v.name.startswith(f"Hisco{v.vendor.hisco_label}_")
        assert v.forecast_tag in v.name
        assert v.flavor.value in v.name


# -----------------------------------------------------------------------------
# forecast_dates_from_seed: dates come from the forecast_factors CSV
# -----------------------------------------------------------------------------

def test_forecast_dates_from_seed_are_distinct_dates():
    seeds = load_all(SEEDS_DIR)
    dates = forecast_dates_from_seed(seeds)
    assert len(dates) > 0
    assert len(dates) == len(set(dates)), "dates must be unique"
    assert all(isinstance(d, date) for d in dates)
    assert dates == sorted(dates), "dates must be returned sorted"


def test_end_to_end_variants_from_real_seed():
    """Every forecast_date in the seed, crossed with both real vendors'
    flavors, produces a valid variant set with unique Hisco names."""
    seeds    = load_all(SEEDS_DIR)
    cfg      = config.resolve()
    variants = build_variants(forecast_dates_from_seed(seeds), cfg.vendors)
    assert len(variants) > 0
    assert len({v.name for v in variants}) == len(variants)


# -----------------------------------------------------------------------------
# Flavor enum integrity
# -----------------------------------------------------------------------------

def test_flavor_enum_has_expected_members():
    assert set(Flavor) == {Flavor.MAIN, Flavor.DIALSUP}


def test_flavor_members_are_strings():
    assert Flavor.MAIN    == "main"
    assert Flavor.DIALSUP == "dialsup"
    assert isinstance(Flavor.MAIN, str)


# -----------------------------------------------------------------------------
# count_event_id_orphans: observation, not a guard
# -----------------------------------------------------------------------------

def _ylt_for_orphan_test(event_ids: list[int]) -> pl.LazyFrame:
    return pl.DataFrame({
        Y.VENDOR:     [VendorName.VERISK] * len(event_ids),
        Y.EVENT_ID:   event_ids,
        Y.YEAR_ID:    [1] * len(event_ids),
        Y.MODEL_CODE: [41] * len(event_ids),
    }, schema={
        Y.VENDOR:     pl.String,
        Y.EVENT_ID:   pl.Int64,
        Y.YEAR_ID:    pl.Int64,
        Y.MODEL_CODE: pl.Int64,
    }).lazy()


def _air_events_seed(event_ids: list[int]) -> pl.LazyFrame:
    return pl.DataFrame({
        AE.EVENT_ID: event_ids,
        AE.MODEL_ID: [41] * len(event_ids),
        AE.EVENT:    event_ids,
        AE.YEAR:     [1] * len(event_ids),
        AE.DAY:      [1] * len(event_ids),
    }, schema={
        AE.EVENT_ID: pl.Int64, AE.MODEL_ID: pl.Int64, AE.EVENT: pl.Int64,
        AE.YEAR: pl.Int64, AE.DAY: pl.Int64,
    }).lazy()


def test_count_event_id_orphans_zero_when_all_match():
    ylt = _ylt_for_orphan_test([10, 20, 30])
    ae  = _air_events_seed([10, 20, 30, 40])
    assert count_event_id_orphans(ylt, ae, vendor_filter=VendorName.VERISK) == 0


def test_count_event_id_orphans_counts_unmatched_rows():
    ylt = _ylt_for_orphan_test([10, 20, 30, 40])
    ae  = _air_events_seed([10, 20])
    assert count_event_id_orphans(ylt, ae, vendor_filter=VendorName.VERISK) == 2


# -----------------------------------------------------------------------------
# _compute_dialsup: zero-guard semantics
# -----------------------------------------------------------------------------

def test_compute_dialsup_returns_zero_when_localccy_loss_is_zero():
    """Documents the zero-guard: a row with zero local-ccy loss yields dialsup=0,
    not divide-by-zero. The composite factor is still meaningful but applied to
    a raw loss that is also zero, so the answer is zero either way."""
    ylt = pl.DataFrame({
        Y.LOSS:                              [0.0],
        M.LOSS_UPLIFTED_CAPPED_LOCALCCY:     [0.0],
        "loss_uplifted_capped_localccy_202601_euws_fagross": [0.0],
    }).lazy()
    out = _compute_dialsup(ylt, ["202601"]).collect()
    assert out["dialsup_202601"][0] == 0.0


def test_compute_dialsup_computes_ratio_when_localccy_nonzero():
    ylt = pl.DataFrame({
        Y.LOSS:                              [100.0],
        M.LOSS_UPLIFTED_CAPPED_LOCALCCY:     [50.0],
        "loss_uplifted_capped_localccy_202601_euws_fagross": [60.0],   # composite = 60/50 = 1.2
    }).lazy()
    out = _compute_dialsup(ylt, ["202601"]).collect()
    assert out["dialsup_202601"][0] == pytest.approx(100.0 * 1.2)
