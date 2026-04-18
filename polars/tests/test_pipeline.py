"""Pipeline scaffolding: VariantSpec + build_variants + seed-driven forecast dates."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from rollup import config
from rollup.config import Flavor, Vendor
from rollup.pipeline import (
    VariantSpec,
    build_variants,
    forecast_dates_from_seed,
)
from rollup.seeds import load_all


SEEDS_DIR = Path(__file__).resolve().parent.parent / "seeds"


# -----------------------------------------------------------------------------
# VariantSpec behaves like a typed triple, not a raw tuple
# -----------------------------------------------------------------------------

def _fake_vendor(name: str, label: str, flavors=None) -> Vendor:
    return Vendor(
        name=name,
        hisco_label=label,
        n_simulations=10_000,
        ylt_dir=Path("/tmp"),
        ylt_glob="*.parquet",
        ep_summary_dir=Path("/tmp"),
        flavors=flavors or (Flavor.STANDARD, Flavor.FAGROSS, Flavor.DIALSUP),
    )


def test_variant_name_follows_hisco_convention():
    v = _fake_vendor("verisk", "AIR")
    spec = VariantSpec(vendor=v, forecast_date=date(2026, 1, 1), flavor=Flavor.FAGROSS)
    assert spec.name == "HiscoAIR_202601_fagross"


def test_variant_forecast_tag_is_yyyymm():
    v = _fake_vendor("risklink", "RMS")
    spec = VariantSpec(vendor=v, forecast_date=date(2027, 7, 1), flavor=Flavor.STANDARD)
    assert spec.forecast_tag == "202707"


def test_variant_loss_metric_per_flavor():
    v = _fake_vendor("verisk", "AIR")
    standard = VariantSpec(v, date(2026, 1, 1), Flavor.STANDARD).loss_metric
    fagross  = VariantSpec(v, date(2026, 1, 1), Flavor.FAGROSS).loss_metric
    dialsup  = VariantSpec(v, date(2026, 1, 1), Flavor.DIALSUP).loss_metric
    assert standard == "loss_uplifted_capped_localccy_202601_euws"
    assert fagross  == "loss_uplifted_capped_localccy_202601_euws_fagross"
    assert dialsup  == "dialsup_202601"


# -----------------------------------------------------------------------------
# build_variants: cross-product, sorted by date, respects each vendor's flavors
# -----------------------------------------------------------------------------

def test_build_variants_cross_product_count():
    vendors = (_fake_vendor("verisk", "AIR"), _fake_vendor("risklink", "RMS"))
    dates = [date(2026, 1, 1), date(2026, 7, 1), date(2027, 1, 1)]
    variants = build_variants(dates, vendors)
    # 2 vendors × 3 dates × 3 flavors
    assert len(variants) == 2 * 3 * 3


def test_build_variants_respects_per_vendor_flavors():
    """A vendor that only emits STANDARD should produce only STANDARD outputs."""
    slim_vendor = _fake_vendor("verisk", "AIR", flavors=(Flavor.STANDARD,))
    variants = build_variants([date(2026, 1, 1), date(2027, 1, 1)], (slim_vendor,))
    assert {v.flavor for v in variants} == {Flavor.STANDARD}
    assert len(variants) == 2


def test_build_variants_names_are_all_unique():
    vendors = (_fake_vendor("verisk", "AIR"), _fake_vendor("risklink", "RMS"))
    dates = [date(2026, 1, 1), date(2026, 7, 1)]
    variants = build_variants(dates, vendors)
    names = [v.name for v in variants]
    assert len(names) == len(set(names))


def test_build_variants_names_match_hisco_pattern():
    """Every name is HiscoXXX_yyyyMM_<flavor>."""
    vendors = (_fake_vendor("verisk", "AIR"), _fake_vendor("risklink", "RMS"))
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
    assert set(Flavor) == {Flavor.STANDARD, Flavor.FAGROSS, Flavor.DIALSUP}


def test_flavor_members_are_strings():
    assert Flavor.STANDARD == "standard"
    assert isinstance(Flavor.FAGROSS, str)
