"""Unit tests for rollup/stages/factors.py — one block per attach_* function.

Each test builds minimal LazyFrames inline so the assertions are
self-contained and fast (no CSV I/O, no pipeline setup).
"""

from __future__ import annotations

import polars as pl
import pytest

from rollup.config import CurrencyCode, VendorName
from rollup.schemas.columns import (
    AllFactorsCol as AF,
    BlendingWeightsCol as BW,
    NormalizedYltCol as Y,
    RefEuwsRankOverridesCol as EO,
    RefEuwsRateFactorsCol as EU,
    RefForecastFactorsCol as FF,
    RefFxRatesCol as FX,
)
from rollup.stages.factors import (
    MissingFxRateError,
    attach_currency,
    attach_euws,
    attach_forecast_factors,
    attach_rank,
    attach_uplift,
)


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

def _ylt(
    *,
    office: str = "UK",
    lob_class: str = "HH",
    rollup_lob: str = "HIC_HH_UK",
    vendor: VendorName = VendorName.VERISK,
    lob_id: int = 1,
    region_peril_id: int = 206,
    peril_name: str = "Europe Winter Storm",
    region: str = "EU",
    peril_family: str = "WS",
    event_id: int = 1001,
    year_id: int = 2026,
    loss: float = 1000.0,
    cds_cat_class_name: str = "HIC UK Household",
) -> pl.LazyFrame:
    """Minimal YLT row with the columns that factor functions require.

    Defaults represent EU Winter Storm (peril_id=206, family=WS) — i.e. a
    non-flood peril where base_model=row vendor.
    """
    return pl.DataFrame({
        Y.VENDOR:              [vendor],
        Y.LOB_ID:              [lob_id],
        Y.ROLLUP_LOB:          [rollup_lob],
        Y.REGION_PERIL_ID:     [region_peril_id],
        Y.PERIL_NAME:          [peril_name],
        Y.REGION:              [region],
        Y.PERIL_FAMILY:        [peril_family],
        Y.EVENT_ID:            [event_id],
        Y.YEAR_ID:             [year_id],
        Y.LOSS:                [loss],
        Y.OFFICE:              [office],
        Y.LOB_CLASS:           [lob_class],
        Y.CDS_CAT_CLASS_NAME:  [cds_cat_class_name],
    }, schema={
        Y.VENDOR:              pl.String,
        Y.LOB_ID:              pl.Int64,
        Y.ROLLUP_LOB:          pl.String,
        Y.REGION_PERIL_ID:     pl.Int64,
        Y.PERIL_NAME:          pl.String,
        Y.REGION:              pl.String,
        Y.PERIL_FAMILY:        pl.String,
        Y.EVENT_ID:            pl.Int64,
        Y.YEAR_ID:             pl.Int64,
        Y.LOSS:                pl.Float64,
        Y.OFFICE:              pl.String,
        Y.LOB_CLASS:           pl.String,
        Y.CDS_CAT_CLASS_NAME:  pl.String,
    }).lazy()


# --------------------------------------------------------------------------- #
# attach_currency                                                             #
# --------------------------------------------------------------------------- #

def _fx_seed(*pairs: tuple[CurrencyCode, float]) -> pl.LazyFrame:
    """Build an FX seed where each pair is (currency_code, rate_to_GBP)."""
    return pl.DataFrame({
        FX.CURRENCY_CODE:   [c for c, _ in pairs],
        FX.TARGET_CURRENCY: [CurrencyCode.GBP] * len(pairs),
        FX.RATE_DATE:       ["2026-01-01"] * len(pairs),
        FX.RATE:            [r for _, r in pairs],
    }, schema={
        FX.CURRENCY_CODE:   pl.String,
        FX.TARGET_CURRENCY: pl.String,
        FX.RATE_DATE:       pl.Date,
        FX.RATE:            pl.Float64,
    }).lazy()


def test_attach_currency_raises_on_missing_fx_rate():
    """A row with required EUR but no EUR rate in the seed must abort the run.
    fill_null(1.0) here would silently inflate losses."""
    ylt = _ylt(cds_cat_class_name="HSA EU Fine Art")
    fx  = _fx_seed((CurrencyCode.GBP, 1.0))   # EUR is missing
    with pytest.raises(MissingFxRateError, match=CurrencyCode.EUR.value):
        attach_currency(ylt, fx).collect()


def test_attach_currency_attaches_rate_when_present():
    ylt = _ylt(cds_cat_class_name="HSA EU Fine Art")
    fx  = _fx_seed((CurrencyCode.GBP, 1.0), (CurrencyCode.EUR, 0.88))
    out = attach_currency(ylt, fx).collect()
    assert out[AF.REQUIRED_CURRENCY][0] == CurrencyCode.EUR
    assert out[AF.RATE_TO_GBP][0]       == pytest.approx(0.88)


# --------------------------------------------------------------------------- #
# attach_forecast_factors                                                     #
# --------------------------------------------------------------------------- #

def _forecast_seed(forecast_date: str, factor: float) -> pl.LazyFrame:
    return pl.DataFrame({
        FF.CLASS:         ["HH"],
        FF.OFFICE:        ["UK"],
        FF.OFFICE_ISO2:   ["UK"],
        FF.BASE_DATE:     ["2026-01-01"],
        FF.FORECAST_DATE: [forecast_date],
        FF.FACTOR:        [factor],
    }, schema={
        FF.CLASS:         pl.String,
        FF.OFFICE:        pl.String,
        FF.OFFICE_ISO2:   pl.String,
        FF.BASE_DATE:     pl.Date,
        FF.FORECAST_DATE: pl.Date,
        FF.FACTOR:        pl.Float64,
    }).lazy()


def test_forecast_factor_attached_for_matching_tag():
    seed = _forecast_seed("2026-01-01", 1.05)
    out  = attach_forecast_factors(_ylt(), seed, ["202601"]).collect()
    assert out["f_202601"][0] == pytest.approx(1.05)


def test_forecast_factor_works_with_mid_month_seed_date():
    """The dt.year/month filter must work for any day-of-month in the seed."""
    seed = _forecast_seed("2026-04-15", 1.08)
    out  = attach_forecast_factors(_ylt(office="UK", lob_class="HH"), seed, ["202604"]).collect()
    assert out["f_202604"][0] == pytest.approx(1.08)


def test_forecast_factor_defaults_to_one_when_lob_missing():
    seed = _forecast_seed("2026-01-01", 1.05)
    ylt  = _ylt(office="FR", lob_class="COMM")   # no row in seed for this office+class
    out  = attach_forecast_factors(ylt, seed, ["202601"]).collect()
    assert out["f_202601"][0] == pytest.approx(1.0)


def test_forecast_multiple_tags_all_attached():
    seed = pl.concat([
        _forecast_seed("2026-01-01", 1.05),
        _forecast_seed("2026-07-01", 1.07),
    ])
    out = attach_forecast_factors(_ylt(), seed, ["202601", "202607"]).collect()
    assert out["f_202601"][0] == pytest.approx(1.05)
    assert out["f_202607"][0] == pytest.approx(1.07)


# --------------------------------------------------------------------------- #
# attach_euws                                                                 #
# --------------------------------------------------------------------------- #

def _euws_seed(event_id: int, occ_year: int, factor: float) -> pl.LazyFrame:
    return pl.DataFrame({
        EU.MODEL_EVENT_ID: [event_id],
        EU.OCC_YEAR:       [occ_year],
        EU.FACTOR:         [factor],
    }, schema={
        EU.MODEL_EVENT_ID: pl.Int64,
        EU.OCC_YEAR:       pl.Int64,
        EU.FACTOR:         pl.Float64,
    }).lazy()


def _euws_overrides(rollup_lob: str, max_rank: int, factor: float) -> pl.LazyFrame:
    return pl.DataFrame({
        EO.ROLLUP_LOB: [rollup_lob],
        EO.MAX_RANK:   [max_rank],
        EO.FACTOR:     [factor],
    }, schema={
        EO.ROLLUP_LOB: pl.String,
        EO.MAX_RANK:   pl.Int64,
        EO.FACTOR:     pl.Float64,
    }).lazy()


def _ylt_with_rank(rollup_lob: str, rnk: int, event_id: int = 1001, year_id: int = 2026) -> pl.LazyFrame:
    """YLT row that already has `rnk` pre-attached (bypasses attach_rank)."""
    df = _ylt(rollup_lob=rollup_lob, event_id=event_id, year_id=year_id).collect()
    return df.with_columns(pl.lit(rnk, dtype=pl.UInt32).alias(AF.RNK)).lazy()


def test_euws_override_fires_when_rnk_within_threshold():
    """HIC_HH_UK rnk=50 (<=100) → override factor 1.0, not the rate-table 1.10."""
    ylt      = _ylt_with_rank("HIC_HH_UK", rnk=50, event_id=1001, year_id=2026)
    euws     = _euws_seed(1001, 2026, 1.10)
    override = _euws_overrides("HIC_HH_UK", 100, 1.0)
    out = attach_euws(ylt, euws, override).collect()
    assert out[AF.EUWS_FACTOR][0] == pytest.approx(1.0)


def test_euws_override_does_not_fire_when_rnk_exceeds_threshold():
    """HIC_HH_UK rnk=150 (>100) → standard rate-table factor 1.10, not override."""
    ylt      = _ylt_with_rank("HIC_HH_UK", rnk=150, event_id=1001, year_id=2026)
    euws     = _euws_seed(1001, 2026, 1.10)
    override = _euws_overrides("HIC_HH_UK", 100, 1.0)
    out = attach_euws(ylt, euws, override).collect()
    assert out[AF.EUWS_FACTOR][0] == pytest.approx(1.10)


def test_euws_no_override_row_uses_rate_table():
    """LOB with no entry in overrides seed falls through to the rate table."""
    ylt      = _ylt_with_rank("HSA_FA_EU_FR", rnk=10, event_id=1001, year_id=2026)
    euws     = _euws_seed(1001, 2026, 0.95)
    override = _euws_overrides("HIC_HH_UK", 100, 1.0)   # different LOB in overrides
    out = attach_euws(ylt, euws, override).collect()
    assert out[AF.EUWS_FACTOR][0] == pytest.approx(0.95)


def test_euws_event_missing_from_rate_table_defaults_to_one():
    """Event not in euws_rate_factors → fill_null → 1.0 (multiplicative pass-through)."""
    ylt      = _ylt_with_rank("HSA_FA_EU_FR", rnk=10, event_id=9999, year_id=2026)
    euws     = _euws_seed(1001, 2026, 0.95)   # different event_id
    override = pl.DataFrame({EO.ROLLUP_LOB: [], EO.MAX_RANK: [], EO.FACTOR: []},
                            schema={EO.ROLLUP_LOB: pl.String, EO.MAX_RANK: pl.Int64,
                                    EO.FACTOR: pl.Float64}).lazy()
    out = attach_euws(ylt, euws, override).collect()
    assert out[AF.EUWS_FACTOR][0] == pytest.approx(1.0)


# --------------------------------------------------------------------------- #
# attach_uplift (now keyed by peril_id directly via blending_weights)         #
# --------------------------------------------------------------------------- #

def _blending_weights(*rows: tuple[int, str | None, VendorName, float]) -> pl.LazyFrame:
    """Long-format blend weights: (peril_id, sub_peril, vendor, weight).

    `peril_name` and `description` are populated with stub values — the
    pipeline never reads them (join is on `peril_id` only) but the schema
    requires them.
    """
    return pl.DataFrame({
        BW.PERIL_ID:    [r[0] for r in rows],
        BW.PERIL_NAME:  [f"peril_{r[0]}" for r in rows],
        BW.DESCRIPTION: ["test fixture" for _ in rows],
        BW.SUB_PERIL:   [r[1] for r in rows],
        BW.VENDOR:      [r[2] for r in rows],
        BW.WEIGHT:      [r[3] for r in rows],
    }, schema={
        BW.PERIL_ID:    pl.Int64,
        BW.PERIL_NAME:  pl.String,
        BW.DESCRIPTION: pl.String,
        BW.SUB_PERIL:   pl.String,
        BW.VENDOR:      pl.String,
        BW.WEIGHT:      pl.Float64,
    }).lazy()


def _empty_blending_weights() -> pl.LazyFrame:
    return pl.DataFrame(schema={
        BW.PERIL_ID:    pl.Int64,
        BW.PERIL_NAME:  pl.String,
        BW.DESCRIPTION: pl.String,
        BW.SUB_PERIL:   pl.String,
        BW.VENDOR:      pl.String,
        BW.WEIGHT:      pl.Float64,
    }).lazy()


def test_uplift_reads_blend_weights_from_seed():
    """blending_weights long-format → vk/rl proportions per peril_id, broadcast to event rows."""
    ylt = _ylt(region_peril_id=206)
    bw  = _blending_weights(
        (206, None, VendorName.VERISK,   0.7),
        (206, None, VendorName.RISKLINK, 0.3),
    )
    out = attach_uplift(ylt, bw).collect()
    assert out[AF.VK_PROPORTION][0] == pytest.approx(0.7, abs=1e-6)
    assert out[AF.RL_PROPORTION][0] == pytest.approx(0.3, abs=1e-6)


def test_uplift_per_peril_blend_is_deterministic():
    """Three perils with different blend weights — each YLT row picks its own peril's weights."""
    bw = _blending_weights(
        (206, None, VendorName.VERISK,   1.0),  (206, None, VendorName.RISKLINK, 0.0),
        (216, None, VendorName.VERISK,   0.5),  (216, None, VendorName.RISKLINK, 0.5),
        (217, None, VendorName.VERISK,   0.0),  (217, None, VendorName.RISKLINK, 1.0),
    )
    ylt = pl.concat([
        _ylt(region_peril_id=206, event_id=1001).collect(),
        _ylt(region_peril_id=216, event_id=1002, peril_family="FL", peril_name="EU Flood").collect(),
        _ylt(region_peril_id=217, event_id=1003, peril_family="FL", peril_name="UK Flood").collect(),
    ]).lazy()

    out = attach_uplift(ylt, bw).collect().sort(Y.REGION_PERIL_ID)
    assert out[AF.VK_PROPORTION][0] == pytest.approx(1.0, abs=1e-6)  # peril 206 — AIR only
    assert out[AF.VK_PROPORTION][1] == pytest.approx(0.5, abs=1e-6)  # peril 216 — 50/50
    assert out[AF.VK_PROPORTION][2] == pytest.approx(0.0, abs=1e-6)  # peril 217 — RMS only


def test_uplift_fallback_when_blending_weights_empty():
    """Stub-empty blending_weights → proportions default to 0.5/0.5 via fill_null."""
    out = attach_uplift(_ylt(), _empty_blending_weights()).collect()
    assert out[AF.VK_PROPORTION][0] == pytest.approx(0.5, abs=1e-6)
    assert out[AF.RL_PROPORTION][0] == pytest.approx(0.5, abs=1e-6)


_N_SIM: dict[VendorName, int] = {VendorName.VERISK: 10_000, VendorName.RISKLINK: 100_000}


def test_uplift_base_model_is_risklink_for_eu_flood():
    """Any peril whose family is FL forces base_model='risklink' regardless of row vendor."""
    ylt = _ylt(vendor=VendorName.RISKLINK, region_peril_id=216,
               peril_name="Europe Flood", peril_family="FL")
    bw  = _blending_weights(
        (216, None, VendorName.VERISK,   0.0),
        (216, None, VendorName.RISKLINK, 1.0),
    )
    out = attach_uplift(ylt, bw, n_sim=_N_SIM).collect()
    assert out[AF.BASE_MODEL][0] == VendorName.RISKLINK


def test_uplift_base_model_is_risklink_for_uk_flood():
    """UK flood (different peril_id but same family) — same rule applies."""
    ylt = _ylt(vendor=VendorName.RISKLINK, region_peril_id=217,
               peril_name="UK Flood", peril_family="FL", region="UK",
               cds_cat_class_name="HIC UK Flood")
    bw  = _blending_weights(
        (217, None, VendorName.VERISK,   0.0),
        (217, None, VendorName.RISKLINK, 1.0),
    )
    out = attach_uplift(ylt, bw, n_sim=_N_SIM).collect()
    assert out[AF.BASE_MODEL][0] == VendorName.RISKLINK


def test_uplift_base_model_is_vendor_for_non_flood():
    """Non-flood perils keep their own vendor as base_model."""
    ylt = _ylt(vendor=VendorName.VERISK, region_peril_id=206,
               peril_family="WS", peril_name="Europe Winter Storm")
    bw  = _blending_weights(
        (206, None, VendorName.VERISK,   0.7),
        (206, None, VendorName.RISKLINK, 0.3),
    )
    out = attach_uplift(ylt, bw, n_sim=_N_SIM).collect()
    assert out[AF.BASE_MODEL][0] == VendorName.VERISK


def test_uplift_factor_blends_aal_from_both_vendors():
    """uplift_factor = blended_AAL / base_model_AAL.

    Setup: lob_id=1, peril_id=206 (EU_WS — verisk base)
        verisk loss=700   → vk_AAL = 700/10000 = 0.07
        risklink loss=300 → rl_AAL = 300/100000 = 0.003
        vk_proportion=0.8, rl_proportion=0.2
        blended_AAL = 0.8*0.07 + 0.2*0.003 = 0.0566
        uplift_factor = 0.0566 / 0.07 ≈ 0.8086
    """
    ylt = pl.concat([
        _ylt(vendor=VendorName.VERISK,   loss=700.0, region_peril_id=206, event_id=1001).collect(),
        _ylt(vendor=VendorName.RISKLINK, loss=300.0, region_peril_id=206, event_id=2001).collect(),
    ]).lazy()
    bw = _blending_weights(
        (206, None, VendorName.VERISK,   0.8),
        (206, None, VendorName.RISKLINK, 0.2),
    )
    out = attach_uplift(ylt, bw, n_sim=_N_SIM).collect()

    vk_aal      = 700 / 10_000
    rl_aal      = 300 / 100_000
    blended_aal = 0.8 * vk_aal + 0.2 * rl_aal
    expected    = blended_aal / vk_aal
    assert out.filter(pl.col(Y.VENDOR) == VendorName.VERISK)[AF.UPLIFT_FACTOR][0] == pytest.approx(expected, rel=1e-5)


def test_uplift_factor_capped_clips_extreme_ratio():
    """A raw uplift_factor > 10 is capped at 10 in uplift_factor_capped."""
    ylt = pl.concat([
        _ylt(vendor=VendorName.VERISK,   loss=1.0,          region_peril_id=206, event_id=1001).collect(),
        _ylt(vendor=VendorName.RISKLINK, loss=10_000_000.0, region_peril_id=206, event_id=2001).collect(),
    ]).lazy()
    bw = _blending_weights(
        (206, None, VendorName.VERISK,   0.5),
        (206, None, VendorName.RISKLINK, 0.5),
    )
    out = attach_uplift(ylt, bw, n_sim=_N_SIM).collect()

    vk_row = out.filter(pl.col(Y.VENDOR) == VendorName.VERISK)
    assert vk_row[AF.UPLIFT_FACTOR][0] > 10.0
    assert vk_row[AF.UPLIFT_FACTOR_CAPPED][0] == pytest.approx(10.0, abs=1e-6)


def test_uplift_factor_defaults_to_one_when_base_model_has_no_events():
    """Flood peril with no risklink rows → rl_AAL=0 → fallback 1.0."""
    ylt = _ylt(vendor=VendorName.VERISK, region_peril_id=216,
               peril_family="FL", peril_name="Europe Flood", loss=500.0)
    bw  = _blending_weights(
        (216, None, VendorName.VERISK,   0.0),
        (216, None, VendorName.RISKLINK, 1.0),
    )
    out = attach_uplift(ylt, bw, n_sim=_N_SIM).collect()
    assert out[AF.UPLIFT_FACTOR][0] == pytest.approx(1.0)