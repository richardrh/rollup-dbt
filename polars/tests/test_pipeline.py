"""Pipeline scaffolding: VariantSpec + build_variants + seed-driven forecast dates."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from rollup import config
from rollup.config import Flavor, Vendor, VendorName
from rollup.pipeline import (
    VariantSpec,
    build_variants,
    count_event_id_orphans,
    count_risklink_event_id_orphans,
    forecast_dates_from_seed,
)
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import MetricCol as M
from rollup.schemas.columns import NormalizedYltCol as Y
from rollup.schemas.columns import RefAirEventsCol as AE
from rollup.schemas.columns import RefRisklinkEventsCol as RLE
from rollup.seeds import load_all


SEEDS_DIR = Path(__file__).resolve().parents[2] / "data" / "seeds"


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
    # MAIN uses the final chain stage; the legacy gross adjustment is gone.
    assert main    == "loss_uplifted_capped_localccy_202601_euws"
    # DIALSUP emits one selected-tag sensitivity column, so no forecast tag in the name.
    assert dialsup == "dialsup"


def test_variant_dialsup_name_has_no_forecast_tag():
    """DIALSUP output file name omits the forecast tag — one file per vendor."""
    v = _fake_vendor(VendorName.VERISK, "AIR")
    spec = VariantSpec(vendor=v, forecast_date=date(2026, 1, 1), flavor=Flavor.DIALSUP)
    assert spec.name == "HiscoAIR_dialsup"
    assert "202601" not in spec.name


# -----------------------------------------------------------------------------
# build_variants: cross-product, sorted by date, respects each vendor's flavors
# -----------------------------------------------------------------------------

def test_build_variants_cross_product_count():
    vendors = (_fake_vendor(VendorName.VERISK, "AIR"), _fake_vendor(VendorName.RISKLINK, "RMS"))
    dates = [date(2026, 1, 1), date(2026, 7, 1), date(2027, 1, 1)]
    variants = build_variants(dates, vendors)
    # 2 vendors × 3 dates × 1 main  +  2 vendors × 1 dialsup  =  6 + 2 = 8
    assert len(variants) == 2 * 3 + 2 * 1


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
    """MAIN names include the forecast tag; DIALSUP names do not."""
    vendors = (_fake_vendor(VendorName.VERISK, "AIR"), _fake_vendor(VendorName.RISKLINK, "RMS"))
    variants = build_variants([date(2026, 1, 1)], vendors)
    for v in variants:
        assert v.name.startswith(f"Hisco{v.vendor.hisco_label}_")
        assert v.flavor.value in v.name
        if v.flavor == Flavor.MAIN:
            assert v.forecast_tag in v.name
        else:
            # DIALSUP: one file per vendor — no date in the filename
            assert v.forecast_tag not in v.name


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


def _risklink_ylt_for_orphan_test(event_ids: list[int]) -> pl.LazyFrame:
    return pl.DataFrame({
        Y.VENDOR:     [VendorName.RISKLINK] * len(event_ids),
        Y.EVENT_ID:   event_ids,
        Y.YEAR_ID:    [1] * len(event_ids),
        Y.MODEL_CODE: [0] * len(event_ids),
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


def _risklink_events_seed(event_ids: list[int]) -> pl.LazyFrame:
    return pl.DataFrame({
        RLE.EVENT_ID: event_ids,
        RLE.YEAR:     [1] * len(event_ids),
        RLE.DAY:      [1] * len(event_ids),
    }, schema={
        RLE.EVENT_ID: pl.Int64, RLE.YEAR: pl.Int64, RLE.DAY: pl.Int64,
    }).lazy()


def test_count_event_id_orphans_zero_when_all_match():
    ylt = _ylt_for_orphan_test([10, 20, 30])
    ae  = _air_events_seed([10, 20, 30, 40])
    assert count_event_id_orphans(ylt, ae, vendor_filter=VendorName.VERISK) == 0


def test_count_event_id_orphans_counts_unmatched_rows(caplog):
    ylt = _ylt_for_orphan_test([10, 20, 30, 40])
    ae  = _air_events_seed([10, 20])
    assert count_event_id_orphans(ylt, ae, vendor_filter=VendorName.VERISK) == 2
    message = caplog.text
    assert "event catalogue validation incomplete" in message
    assert "data/seeds/validation/verisk_events.parquet" in message
    assert "ModelEventDay remains 0" in message


def test_count_risklink_event_id_orphans_counts_unmatched_rows(caplog):
    ylt = _risklink_ylt_for_orphan_test([10, 20, 30, 40])
    events = _risklink_events_seed([10, 20])

    assert count_risklink_event_id_orphans(ylt, events) == 2
    message = caplog.text
    assert "event catalogue validation incomplete" in message
    assert "vendor=risklink" in message
    assert "risklink_flood22_model_events.parquet" in message


def test_count_risklink_event_id_orphans_zero_when_all_match():
    ylt = _risklink_ylt_for_orphan_test([10, 20, 30])
    events = _risklink_events_seed([10, 20, 30, 40])

    assert count_risklink_event_id_orphans(ylt, events) == 0


# -----------------------------------------------------------------------------
# min_loss filter — fanout_hisco + audit_long
# -----------------------------------------------------------------------------

def test_resolve_picks_up_min_loss_env(monkeypatch):
    """`ROLLUP_MIN_LOSS=2500` env var → cfg.min_loss == 2500.0 (overrides default)."""
    monkeypatch.setenv(config.EnvVar.MIN_LOSS, "2500")
    cfg = config.resolve()
    assert cfg.min_loss == 2500.0


def test_resolve_default_min_loss_is_1000(monkeypatch):
    """Absent env var + no local config → 1000.0 (production default)."""
    monkeypatch.delenv(config.EnvVar.MIN_LOSS, raising=False)
    cfg = config.resolve()
    assert cfg.min_loss == 1000.0


def test_resolve_min_loss_zero_disables_filter(monkeypatch):
    """Explicit ROLLUP_MIN_LOSS=0 disables the filter (overrides default)."""
    monkeypatch.setenv(config.EnvVar.MIN_LOSS, "0")
    cfg = config.resolve()
    assert cfg.min_loss == 0.0


def test_audit_long_filters_below_min_loss():
    """audit_long with min_loss > 0 drops metric rows whose value < threshold."""
    from rollup.pipeline import audit_long

    # Build a tiny all_factors frame with the cols audit_long requires.
    # Using just two metrics — one above, one below — so we can assert exact filtering.
    n = 1
    af = pl.DataFrame({
        AF.VENDOR:                ["verisk"],
        AF.LOB_ID:                [1],
        AF.MODELLED_LOB:          ["X"],
        AF.ROLLUP_LOB:            ["X"],
        AF.LOB_TYPE:              ["t"],
        AF.CDS_CAT_CLASS_NAME:    ["c"],
        AF.REGION_PERIL_ID:       [1],
        AF.MODELLED_REGION_PERIL: ["m"],
        AF.PERIL_NAME:            ["p"],
        AF.REGION:                ["r"],
        AF.PERIL_FAMILY:          ["FL"],
        AF.YEAR_ID:               [1],
        AF.EVENT_ID:              [1],
        AF.MODEL_EVENT_ID:        [1],
        AF.MODEL_CODE:            [0],
        AF.RNK:                   [1],
        AF.RP:                    [1000.0],
        AF.RP_BUCKET:             [1000],
        AF.RL_PROPORTION:         [0.5],
        AF.VK_PROPORTION:         [0.5],
        AF.BASE_MODEL:            ["verisk"],
        # Metric cols audit_long unpivots over (no forecast tags = year-invariant only)
        Y.LOSS:                   [500.0],     # below 1000 — should be dropped
        M.LOSS_UPLIFTED:          [500.0],     # below 1000 — should be dropped
        M.LOSS_UPLIFTED_CAPPED:   [2500.0],    # above 1000 — should be kept
        M.LOSS_UPLIFTED_CAPPED_LOCALCCY: [2500.0],
        "dialsup":                [50.0],      # below 1000 — should be dropped
    }).lazy()
    _ = n  # silence unused-var warning

    out = audit_long(af, tags=[], min_loss=1000.0).collect()
    assert out.height == 2, f"expected 2 rows kept (≥1000), got {out.height}"
    assert (out["value"] >= 1000.0).all()


def test_audit_long_no_filter_when_min_loss_zero():
    """min_loss=0 means no filter — every metric row survives."""
    from rollup.pipeline import audit_long

    af = pl.DataFrame({
        AF.VENDOR:                ["verisk"],
        AF.LOB_ID:                [1],
        AF.MODELLED_LOB:          ["X"],
        AF.ROLLUP_LOB:            ["X"],
        AF.LOB_TYPE:              ["t"],
        AF.CDS_CAT_CLASS_NAME:    ["c"],
        AF.REGION_PERIL_ID:       [1],
        AF.MODELLED_REGION_PERIL: ["m"],
        AF.PERIL_NAME:            ["p"],
        AF.REGION:                ["r"],
        AF.PERIL_FAMILY:          ["FL"],
        AF.YEAR_ID:               [1],
        AF.EVENT_ID:              [1],
        AF.MODEL_EVENT_ID:        [1],
        AF.MODEL_CODE:            [0],
        AF.RNK:                   [1],
        AF.RP:                    [1000.0],
        AF.RP_BUCKET:             [1000],
        AF.RL_PROPORTION:         [0.5],
        AF.VK_PROPORTION:         [0.5],
        AF.BASE_MODEL:            ["verisk"],
        Y.LOSS:                   [500.0],
        M.LOSS_UPLIFTED:          [500.0],
        M.LOSS_UPLIFTED_CAPPED:   [2500.0],
        M.LOSS_UPLIFTED_CAPPED_LOCALCCY: [2500.0],
        "dialsup":                [50.0],
    }).lazy()

    out = audit_long(af, tags=[], min_loss=0.0).collect()
    # 5 metric columns × 1 event = 5 rows
    assert out.height == 5
