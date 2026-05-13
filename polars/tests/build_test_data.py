"""Generate a tiny internally-consistent dataset under `tests/data/`.

Run once via `python tests/build_test_data.py` (or invoke from a pytest fixture).
Produces every file the pipeline needs so `python -m rollup.pipeline --yes`
(with ROLLUP_DATA_DIR + ROLLUP_SEEDS_DIR pointed here) runs end-to-end.

Shape choices:
  * 2 vendors, 2 lobs, 2 perils, 10 simulation years, 2 events/year.
  * Three arbitrary future forecast dates (2026-01, 2026-07, 2027-01) — the
    pipeline derives variants from whatever dates appear in the seed, so
    this number is not load-bearing on production code.
  * Every join resolves. No nulls leaked into downstream math.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from rollup.config import CurrencyCode, VendorName
from rollup.schemas.columns import AnalysesCol as AN
from rollup.schemas.columns import BlendingWeightsCol as BW
from rollup.schemas.columns import EpType
from rollup.schemas.columns import PerilsCol as P
from rollup.schemas.columns import RawRisklinkYltCol as RLK
from rollup.schemas.columns import RawVeriskYltCol as VK
from rollup.schemas.columns import RefAirEventsCol as AE
from rollup.schemas.columns import RefEuwsRankOverridesCol as EO
from rollup.schemas.columns import RefEuwsRateFactorsCol as EU
from rollup.schemas.columns import RefForecastFactorsCol as FF
from rollup.schemas.columns import RefFxRatesCol as FX
from rollup.schemas.columns import RefLobsCol as LB
from rollup.schemas.columns import RefRisklinkEventsCol as RLE
from rollup.schemas.columns import StgRisklinkEpCol as REP
from rollup.schemas.columns import StgVeriskEpCol as VEP
from rollup.schemas.columns import ValidAnalysesCol as VA


TESTS_DIR = Path(__file__).resolve().parent
DATA_DIR  = TESTS_DIR / "data"
SEEDS     = DATA_DIR / "seeds"
YLT_V     = DATA_DIR / "ylt" / VendorName.VERISK
YLT_R     = DATA_DIR / "ylt" / VendorName.RISKLINK
EP_V      = DATA_DIR / "ep_summaries" / VendorName.VERISK
EP_R      = DATA_DIR / "ep_summaries" / VendorName.RISKLINK
OUTPUT    = DATA_DIR / "output"


# --------------------------------------------------------------------------- #
# Synthetic domain                                                            #
# --------------------------------------------------------------------------- #

LOBS = [
    # lob_id, modelled_lob, rollup_lob, lob_type, cds, office, class
    (1, "HIC_HH_UK",     "HIC_HH_UK",     "prop", "HIC UK Household",  "UK", "HH"),
    (2, "HSA_FA_EU_FR",  "HSA_FA_EU_FR",  "fa",   "HSA EU Fine Art",   "FR", "FA"),
]

PERILS = [
    # peril_id, name, region, peril_family
    (206, "Europe Winter Storm", "EU", "WS"),
    (216, "Europe Flood",        "EU", "FL"),
]

# (vendor, analysis_id, modelled_label, peril_id, lob_id)
ANALYSES = [
    (VendorName.VERISK,   "900003", "EU_WS",  206, None),
    (VendorName.VERISK,   "900002", "EU_FL",  216, None),
    (VendorName.RISKLINK, "501",    "EU_WS",  206, 1),
    (VendorName.RISKLINK, "502",    "EU_FL",  216, 2),
]

# Vendor-native analysis IDs that may contribute YLT/EP rows.
VALID_ANALYSES = [
    (VendorName.VERISK,   "900003"),
    (VendorName.VERISK,   "900002"),
    (VendorName.RISKLINK, "501"),
    (VendorName.RISKLINK, "502"),
]

# Simple 50/50 blend on both perils. sub_peril=None → applies to all sub-perils.
# (peril_id, return_period, peril_name, description, sub_peril, vendor, base_model, weight)
BLENDING_WEIGHTS = [
    (206, rp, "Europe Winter Storm", "default 50/50 blend", None, vendor, VendorName.VERISK, 0.5)
    for rp in (0, 200, 1000, 10000)
    for vendor in (VendorName.VERISK, VendorName.RISKLINK)
] + [
    (216, rp, "Europe Flood", "default 50/50 blend", None, vendor, VendorName.RISKLINK, 0.5)
    for rp in (0, 200, 1000, 10000)
    for vendor in (VendorName.VERISK, VendorName.RISKLINK)
]

# Three arbitrary forecast dates — the pipeline picks them up from the seed.
FORECAST_DATES = [date(2026, 1, 1), date(2026, 7, 1), date(2027, 1, 1)]
# (class, office, office_iso2, forecast_date, factor)
FORECAST_FACTORS = []
for fd in FORECAST_DATES:
    FORECAST_FACTORS += [
        ("HH", "UK", "UK", fd, 1.05 if fd.year == 2026 else 1.10),
        ("FA", "FR", "FR", fd, 1.02 if fd.year == 2026 else 1.06),
    ]

FX_RATES = [
    (CurrencyCode.GBP, CurrencyCode.GBP, date(2026, 1, 1), 1.0),
    (CurrencyCode.EUR, CurrencyCode.GBP, date(2026, 1, 1), 0.88),
    # USD is not in CurrencyCode (no derivation rule emits it) — left as raw
    # string. The seed accepts any code; only the in-code derivation in
    # attach_currency is the closed set.
    ("USD",            CurrencyCode.GBP, date(2026, 1, 1), 0.80),
]

# Small euws table — enough to join to a subset of Verisk events.
EUWS_ROWS: list[tuple[int, int, float]] = [
    (1001, 2026, 1.10),
    (1002, 2026, 0.95),
]

# Rank-threshold overrides for EUWS (mirrors the real seeds/euws_rank_overrides.csv).
EUWS_RANK_OVERRIDES = [
    ("HIC_HH_UK", 100, 1.0),
]

# AIR events catalogue — every Verisk YLT event_id gets a day-of-year.
AIR_EVENTS: list[tuple[int, int, int, int, int]] = []


# --------------------------------------------------------------------------- #
# Writers                                                                     #
# --------------------------------------------------------------------------- #

def _mkdirs() -> None:
    for p in [
        SEEDS / "business", SEEDS / "vor", SEEDS / "adjustments", SEEDS / "validation",
        YLT_V, YLT_R, EP_V, EP_R, OUTPUT,
    ]:
        p.mkdir(parents=True, exist_ok=True)


def _write_seeds() -> None:
    # business/
    pl.DataFrame(LOBS, orient="row", schema=[
        LB.LOB_ID, LB.MODELLED_LOB, LB.ROLLUP_LOB, LB.LOB_TYPE,
        LB.CDS_CAT_CLASS_NAME, LB.OFFICE, LB.CLASS,
    ]).write_csv(SEEDS / "business/lobs.csv")

    pl.DataFrame(PERILS, orient="row", schema=[
        P.PERIL_ID, P.NAME, P.REGION, P.PERIL_FAMILY,
    ]).write_csv(SEEDS / "business/perils.csv")

    pl.DataFrame(ANALYSES, orient="row", schema=[
        AN.VENDOR, AN.ANALYSIS_ID, AN.MODELLED_LABEL, AN.PERIL_ID, AN.LOB_ID,
    ]).write_csv(SEEDS / "business/analyses.csv")

    pl.DataFrame(VALID_ANALYSES, orient="row", schema=[
        VA.VENDOR, VA.ANALYSIS_ID,
    ]).write_csv(SEEDS / "business/valid_analyses.csv")

    # vor/
    pl.DataFrame(BLENDING_WEIGHTS, orient="row", schema=[
        BW.PERIL_ID, BW.RETURN_PERIOD, BW.PERIL_NAME, BW.DESCRIPTION,
        BW.SUB_PERIL, BW.VENDOR, BW.BASE_MODEL, BW.WEIGHT,
    ]).write_csv(SEEDS / "vor/blending_weights.csv")

    pl.DataFrame(FORECAST_FACTORS, orient="row", schema=[
        FF.CLASS, FF.OFFICE, FF.OFFICE_ISO2, FF.FORECAST_DATE, FF.FACTOR,
    ]).write_csv(SEEDS / "vor/forecast_factors.csv")

    pl.DataFrame(FX_RATES, orient="row", schema=[
        FX.CURRENCY_CODE, FX.TARGET_CURRENCY, FX.RATE_DATE, FX.RATE,
    ]).write_csv(SEEDS / "vor/fx_rates.csv")

    pl.DataFrame(EUWS_ROWS, orient="row", schema=[
        EU.MODEL_EVENT_ID, EU.OCC_YEAR, EU.FACTOR,
    ]).write_csv(SEEDS / "vor/euws_rate_factors.csv")

    # adjustments/
    pl.DataFrame(EUWS_RANK_OVERRIDES, orient="row", schema=[
        EO.ROLLUP_LOB, EO.MAX_RANK, EO.FACTOR,
    ]).write_csv(SEEDS / "adjustments/euws_rank_overrides.csv")

    # validation/ stubs — air_events populated by _write_air_events after YLTs are known
    pl.DataFrame(schema={
        RLE.EVENT_ID: pl.Int64,
        RLE.YEAR:     pl.Int64,
        RLE.DAY:      pl.Int64,
    }).write_csv(SEEDS / "validation/risklink_events.csv")


def _write_verisk_ylt() -> list[tuple[int, int, int]]:
    """Returns the list of (event_id, year_id, model_code) triples written
    (used to build a coherent air_events seed)."""
    triples: list[tuple[int, int, int]] = []
    rows = []
    # Analysis × ExposureAttribute × ModelCode × year × local event
    cases = [
        ("EU_WS", "HIC_HH_UK",    41),
        ("EU_FL", "HSA_FA_EU_FR", 92),
    ]
    eid = 1000
    for analysis, lob, model_code in cases:
        for year_id in range(1, 11):
            for _ in range(2):
                eid += 1
                triples.append((eid, year_id, model_code))
                loss = 100.0 * year_id + (eid % 7) * 13.0   # deterministic spread
                rows.append({
                    VK.ANALYSIS:           analysis,
                    VK.EXPOSURE_ATTRIBUTE: lob,
                    VK.CATALOG_TYPE_CODE:  "STC",
                    VK.EVENT_ID:           eid,
                    VK.MODEL_CODE:         model_code,
                    VK.YEAR_ID:            year_id,
                    VK.PERILSET_CODE:      1,
                    VK.GROUND_UP_LOSS:     loss * 1.2,
                    VK.GROSS_LOSS:         loss * 1.1,
                    VK.NET_PRE_CAT_LOSS:   loss,
                    VK.FILENAME:           "synthetic",
                })
    pl.DataFrame(rows).write_parquet(YLT_V / "air_ylt_test.parquet")
    return triples


def _write_risklink_ylt() -> None:
    rows = []
    eid = 5000
    for anls_id in (501, 502):
        for year_id in range(1, 11):
            for _ in range(2):
                eid += 1
                loss = 150.0 * year_id + (eid % 5) * 17.0
                rows.append({
                    RLK.SIMULATION_SET_ID: 1,
                    RLK.YEAR_ID:           year_id,
                    RLK.EVENT_ID:          eid,
                    RLK.DATE:              f"2026-{(year_id % 12) + 1:02d}-15",
                    RLK.P_VALUE:           0.5,
                    RLK.ANLS_ID:           anls_id,
                    RLK.NAME:              "synthetic",
                    RLK.DESCRIPTION:       "synthetic",
                    RLK.RATE:              0.01,
                    RLK.MEAN_LOSS:         loss,
                    RLK.STD_DEV:           loss * 0.1,
                    RLK.EXP_VALUE:         loss,
                    RLK.LOSS:              loss,
                })
    pl.DataFrame(rows).write_parquet(YLT_R / "risklink_ylt_test.parquet")


def _write_air_events(verisk_triples: list[tuple[int, int, int]]) -> None:
    """One air_events row per unique (event_id, year, model_id) triple that
    appears in the Verisk YLT. `count_event_id_orphans` will report 0."""
    rows = []
    for eid, year_id, model_code in verisk_triples:
        rows.append({
            AE.EVENT_ID: eid,
            AE.MODEL_ID: model_code,
            AE.EVENT:    eid,
            AE.YEAR:     year_id,
            AE.DAY:      ((eid % 365) + 1),
        })
    pl.DataFrame(rows, schema={
        AE.EVENT_ID: pl.Int64, AE.MODEL_ID: pl.Int64, AE.EVENT: pl.Int64,
        AE.YEAR: pl.Int64, AE.DAY: pl.Int64,
    }).write_csv(SEEDS / "validation/air_events.csv")


def _write_ep_summaries() -> None:
    """Tiny EP summaries — one row per (vendor, lob, peril, ep_type, rp)."""
    verisk_rows = []
    for analysis, lob in (("EU_WS", "HIC_HH_UK"), ("EU_FL", "HSA_FA_EU_FR")):
        for (rp, ep_type, gl) in [(0,    EpType.AAL, 100.0),
                                  (200,  EpType.OEP, 500.0),
                                  (1000, EpType.OEP, 1500.0)]:
            verisk_rows.append({
                VEP.RP:       rp,
                VEP.EP_TYPE:  ep_type,
                VEP.ANALYSIS: analysis,
                VEP.LOB:      lob,
                VEP.GL:       gl,
            })
    pl.DataFrame(verisk_rows).write_csv(EP_V / "verisk_ep.csv")

    rl_rows = []
    for region_peril in ("EU_WS", "EU_FL"):
        lob = "HIC_HH_UK" if region_peril == "EU_WS" else "HSA_FA_EU_FR"
        analysis_id = 501 if region_peril == "EU_WS" else 502
        for (rp, ep_type, gl) in [(0,    EpType.AAL, 120.0),
                                  (200,  EpType.OEP, 550.0),
                                  (1000, EpType.OEP, 1600.0)]:
            rl_rows.append({
                REP.ID:           analysis_id,
                REP.RP:           rp,
                REP.EP_TYPE:      ep_type,
                REP.LOB:          lob,
                REP.REGION_PERIL: region_peril,
                REP.GL:           gl,
            })
    pl.DataFrame(rl_rows).write_csv(EP_R / "risklink_ep.csv")


# --------------------------------------------------------------------------- #
# Entry                                                                       #
# --------------------------------------------------------------------------- #

def build() -> Path:
    """Build everything. Returns the data root so callers can point env at it."""
    _mkdirs()
    _write_seeds()
    verisk_triples = _write_verisk_ylt()
    _write_risklink_ylt()
    _write_air_events(verisk_triples)
    _write_ep_summaries()
    return DATA_DIR


if __name__ == "__main__":
    root = build()
    print(f"Built test data under {root}")
