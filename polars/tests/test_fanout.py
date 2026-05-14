"""Tests for Hisco fanout projections."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from rollup.config import Flavor, Vendor, VendorName
from rollup.marts import VariantSpec, fanout_hisco
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import HiscoFanoutCol as H
from rollup.schemas.columns import RefAirEventsCol as AE
from rollup.schemas.columns import RefRisklinkEventsCol as RLE


_METRIC = "loss_uplifted_capped_localccy_202601_euws"


def _vendor(name: VendorName) -> Vendor:
    return Vendor(
        name=name,
        hisco_label="RMS" if name == VendorName.RISKLINK else "AIR",
        n_simulations=100_000 if name == VendorName.RISKLINK else 10_000,
        ylt_dir=Path("/tmp/ignored"),
        ylt_glob="*",
        ep_summary_dir=Path("/tmp/ignored"),
    )


def _variant(name: VendorName) -> VariantSpec:
    return VariantSpec(_vendor(name), date(2026, 1, 1), Flavor.MAIN)


def _all_factors(name: VendorName) -> pl.LazyFrame:
    return pl.DataFrame({
        AF.BASE_MODEL:         [name.value, name.value, name.value],
        AF.MODEL_EVENT_ID:     [10, 20, 30],
        AF.EVENT_ID:           [10, 20, 30],
        AF.YEAR_ID:            [1, 1, 2],
        AF.MODEL_CODE:         [41, 41, 41],
        AF.REQUIRED_CURRENCY:  ["GBP", "GBP", "GBP"],
        AF.CDS_CAT_CLASS_NAME: ["class", "class", "class"],
        _METRIC:               [100.0, 200.0, 300.0],
    }).lazy()


def _air_events() -> pl.LazyFrame:
    return pl.DataFrame({
        AE.EVENT_ID: [1000010, 1000020],
        AE.MODEL_ID: [41, 41],
        AE.EVENT:    [10, 20],
        AE.YEAR:     [1, 1],
        AE.DAY:      [11, 22],
    }).lazy()


def _risklink_events() -> pl.LazyFrame:
    return pl.DataFrame({
        RLE.EVENT_ID: [10, 10, 20, 30],
        RLE.YEAR:     [1, 1, 1, 3],
        RLE.DAY:      [5, 6, 8, 9],
    }).lazy()


def test_risklink_fanout_withdayid_inner_joins_on_event_and_year():
    out = fanout_hisco(
        _all_factors(VendorName.RISKLINK),
        _variant(VendorName.RISKLINK),
        risklink_events=_risklink_events(),
    ).collect()

    assert out.height == 3
    assert out[H.MODEL_EVENT_ID].to_list() == [10, 10, 20]
    assert out[H.MODEL_YEAR].to_list() == [1, 1, 1]
    assert out[H.MODEL_EVENT_DAY].to_list() == [5, 6, 8]


def test_risklink_fanout_without_event_catalogue_keeps_zero_day_fallback():
    out = fanout_hisco(_all_factors(VendorName.RISKLINK), _variant(VendorName.RISKLINK)).collect()

    assert out.height == 3
    assert out[H.MODEL_EVENT_DAY].to_list() == [0, 0, 0]


def test_verisk_fanout_enriches_model_event_id_and_day_from_air_events():
    out = fanout_hisco(
        _all_factors(VendorName.VERISK),
        _variant(VendorName.VERISK),
        air_events=_air_events(),
    ).collect()

    assert out.height == 3
    assert out[H.MODEL_EVENT_ID].to_list() == [1000010, 1000020, None]
    assert out[H.MODEL_EVENT_DAY].to_list() == [11, 22, None]


def test_verisk_fanout_ignores_risklink_event_catalogue_without_air_events():
    out = fanout_hisco(
        _all_factors(VendorName.VERISK),
        _variant(VendorName.VERISK),
        risklink_events=_risklink_events(),
    ).collect()

    assert out.height == 3
    assert out[H.MODEL_EVENT_DAY].to_list() == [0, 0, 0]
