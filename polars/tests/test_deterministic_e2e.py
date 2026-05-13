"""Deterministic CLI E2E with hand-checkable blending math.

The fixture writes fake EP-summary long CSVs, fake vendor YLT parquets, and the
minimal seeds needed by the CLI.  Runtime blending is intentionally seeded as a
50/50 blend so the expected numbers can be calculated by inspection:

    Verisk AAL   = 10,000,000 / 10,000 = 1,000
    RiskLink AAL = 50,000,000 / 100,000 = 500
    50/50 blend  = 0.5 * 1,000 + 0.5 * 500 = 750
    uplift       = blended / base_model_verisk = 750 / 1,000 = 0.75

Therefore Verisk base losses 100, 200, and 300 should fan out as 75, 150, and
225 when all other factors are 1.0.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from rollup import config
from rollup.cli import main as cli_main
from rollup.config import CurrencyCode, EnvVar, VendorName
from rollup.schemas.columns import (
    AllFactorsCol as AF,
    AnalysesCol as AN,
    BlendingWeightsCol as BW,
    EpType,
    HiscoFanoutCol as H,
    PerilsCol as P,
    RawRisklinkYltCol as RLK,
    RawVeriskYltCol as VK,
    RefAirEventsCol as AE,
    RefEuwsRankOverridesCol as EO,
    RefEuwsRateFactorsCol as EU,
    RefForecastFactorsCol as FF,
    RefFxRatesCol as FX,
    RefLobsCol as LB,
    RefRisklinkEventsCol as RLE,
    StgRisklinkEpCol as REP,
    StgVeriskEpCol as VEP,
    ValidAnalysesCol as VA,
)


def _mkdirs(root: Path) -> None:
    for path in [
        root / "seeds" / "business",
        root / "seeds" / "vor",
        root / "seeds" / "adjustments",
        root / "seeds" / "validation",
        root / "ylt" / VendorName.VERISK,
        root / "ylt" / VendorName.RISKLINK,
        root / "ep_summaries" / VendorName.VERISK,
        root / "ep_summaries" / VendorName.RISKLINK,
        root / "output",
    ]:
        path.mkdir(parents=True, exist_ok=True)


def _write_minimal_seeds(root: Path) -> None:
    seeds = root / "seeds"
    pl.DataFrame({
        LB.LOB_ID: [1],
        LB.MODELLED_LOB: ["LOB_A"],
        LB.ROLLUP_LOB: ["LOB_A"],
        LB.LOB_TYPE: ["prop"],
        LB.CDS_CAT_CLASS_NAME: ["LOB UK Test"],
        LB.OFFICE: ["UK"],
        LB.CLASS: ["HH"],
    }).write_csv(seeds / "business" / "lobs.csv")

    pl.DataFrame({
        P.PERIL_ID: [1],
        P.NAME: ["Europe Wind"],
        P.REGION: ["EU"],
        P.PERIL_FAMILY: ["WS"],
    }).write_csv(seeds / "business" / "perils.csv")

    pl.DataFrame({
        AN.VENDOR: [VendorName.VERISK, VendorName.RISKLINK],
        AN.ANALYSIS_ID: ["EU_WS", "501"],
        AN.MODELLED_LABEL: ["EU_WS", "EU_WS"],
        AN.PERIL_ID: [1, 1],
        AN.LOB_ID: [None, 1],
    }, schema={
        AN.VENDOR: pl.String,
        AN.ANALYSIS_ID: pl.String,
        AN.MODELLED_LABEL: pl.String,
        AN.PERIL_ID: pl.Int64,
        AN.LOB_ID: pl.Int64,
    }).write_csv(seeds / "business" / "analyses.csv")

    pl.DataFrame({
        VA.VENDOR: [VendorName.VERISK, VendorName.RISKLINK],
        VA.ANALYSIS_ID: ["EU_WS", "501"],
    }).write_csv(seeds / "business" / "valid_analyses.csv")

    blend_rows = [
        (1, rp, "Europe Wind", "deterministic 50/50", None, vendor, VendorName.VERISK, 0.5)
        for rp in (0, 200, 1000, 10000)
        for vendor in (VendorName.VERISK, VendorName.RISKLINK)
    ]
    pl.DataFrame(blend_rows, orient="row", schema=[
        BW.PERIL_ID, BW.RETURN_PERIOD, BW.PERIL_NAME, BW.DESCRIPTION,
        BW.SUB_PERIL, BW.VENDOR, BW.BASE_MODEL, BW.WEIGHT,
    ]).write_csv(seeds / "vor" / "blending_weights.csv")

    pl.DataFrame({
        FF.CLASS: ["HH"],
        FF.OFFICE: ["UK"],
        FF.OFFICE_ISO2: ["UK"],
        FF.FORECAST_DATE: [date(2026, 1, 1)],
        FF.FACTOR: [1.0],
    }).write_csv(seeds / "vor" / "forecast_factors.csv")

    pl.DataFrame({
        FX.CURRENCY_CODE: [CurrencyCode.GBP, CurrencyCode.EUR],
        FX.TARGET_CURRENCY: [CurrencyCode.GBP, CurrencyCode.GBP],
        FX.RATE_DATE: [date(2026, 1, 1), date(2026, 1, 1)],
        FX.RATE: [1.0, 1.0],
    }).write_csv(seeds / "vor" / "fx_rates.csv")

    pl.DataFrame({
        EU.MODEL_EVENT_ID: [1, 2, 3, 4],
        EU.OCC_YEAR: [1, 1, 1, 1],
        EU.FACTOR: [1.0, 1.0, 1.0, 1.0],
    }).write_csv(seeds / "vor" / "euws_rate_factors.csv")

    pl.DataFrame({EO.ROLLUP_LOB: ["NO_MATCH"], EO.MAX_RANK: [0], EO.FACTOR: [1.0]}).write_csv(
        seeds / "adjustments" / "euws_rank_overrides.csv"
    )
    pl.DataFrame({
        AE.EVENT_ID: [1, 2, 3, 4],
        AE.MODEL_ID: [41, 41, 41, 41],
        AE.EVENT: [1, 2, 3, 4],
        AE.YEAR: [1, 1, 1, 1],
        AE.DAY: [1, 2, 3, 4],
    }).write_csv(seeds / "validation" / "air_events.csv")
    pl.DataFrame(schema={RLE.EVENT_ID: pl.Int64, RLE.YEAR: pl.Int64, RLE.DAY: pl.Int64}).write_csv(
        seeds / "validation" / "risklink_events.csv"
    )


def _write_fake_ep_summaries(root: Path) -> None:
    """Write obvious EP fixtures used by the plan/CLI, including 500/1000 rows."""
    pl.DataFrame({
        VEP.RP: [0],
        VEP.EP_TYPE: [EpType.AAL],
        VEP.ANALYSIS: ["EU_WS"],
        VEP.LOB: ["LOB_A"],
        VEP.GL: [1000.0],
    }).write_csv(root / "ep_summaries" / VendorName.VERISK / "verisk.long.csv")
    pl.DataFrame({
        REP.ID: [501],
        REP.RP: [0],
        REP.EP_TYPE: [EpType.AAL],
        REP.LOB: ["LOB_A"],
        REP.REGION_PERIL: ["EU_WS"],
        REP.GL: [500.0],
    }).write_csv(root / "ep_summaries" / VendorName.RISKLINK / "risklink.long.csv")


def _write_fake_ylts(root: Path) -> None:
    verisk_losses = [100.0, 200.0, 300.0, 9_999_400.0]
    pl.DataFrame({
        VK.ANALYSIS: ["EU_WS"] * 4,
        VK.EXPOSURE_ATTRIBUTE: ["LOB_A"] * 4,
        VK.CATALOG_TYPE_CODE: ["STC"] * 4,
        VK.EVENT_ID: [1, 2, 3, 4],
        VK.MODEL_CODE: [41] * 4,
        VK.YEAR_ID: [1] * 4,
        VK.PERILSET_CODE: [1] * 4,
        VK.GROUND_UP_LOSS: verisk_losses,
        VK.GROSS_LOSS: verisk_losses,
        VK.NET_PRE_CAT_LOSS: verisk_losses,
        VK.FILENAME: ["fake"] * 4,
    }).write_parquet(root / "ylt" / VendorName.VERISK / "air_ylt_fake.parquet")

    risklink_losses = [50_000_000.0]
    pl.DataFrame({
        RLK.SIMULATION_SET_ID: [1],
        RLK.YEAR_ID: [1],
        RLK.EVENT_ID: [9001],
        RLK.DATE: ["2026-01-01"],
        RLK.P_VALUE: [1.0],
        RLK.ANLS_ID: [501],
        RLK.NAME: ["fake"],
        RLK.DESCRIPTION: ["fake"],
        RLK.RATE: [1.0],
        RLK.MEAN_LOSS: risklink_losses,
        RLK.STD_DEV: [0.0],
        RLK.EXP_VALUE: risklink_losses,
        RLK.LOSS: risklink_losses,
    }).write_parquet(root / "ylt" / VendorName.RISKLINK / "risklink_ylt_fake.parquet")


@pytest.fixture
def deterministic_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "deterministic-rollup"
    _mkdirs(root)
    _write_minimal_seeds(root)
    _write_fake_ep_summaries(root)
    _write_fake_ylts(root)

    monkeypatch.setenv(EnvVar.DATA_DIR, str(root))
    monkeypatch.setenv(EnvVar.SEEDS_DIR, str(root / "seeds"))
    monkeypatch.setenv(EnvVar.YLT_VERISK_DIR, str(root / "ylt" / VendorName.VERISK))
    monkeypatch.setenv(EnvVar.YLT_RISKLINK_DIR, str(root / "ylt" / VendorName.RISKLINK))
    monkeypatch.setenv(EnvVar.EP_VERISK_DIR, str(root / "ep_summaries" / VendorName.VERISK))
    monkeypatch.setenv(EnvVar.EP_RISKLINK_DIR, str(root / "ep_summaries" / VendorName.RISKLINK))
    monkeypatch.setenv(EnvVar.OUTPUT_DIR, str(root / "output"))
    monkeypatch.setenv(EnvVar.MIN_LOSS, "0")
    return root


def test_cli_pipeline_applies_hand_calculated_50_50_blend(deterministic_root: Path):
    assert cli_main(["--dry-run", "-y"]) == 0
    assert cli_main(["-y", "--min-loss", "0", "--dump-interim", "--no-derive-blending"]) == 0

    wide = pl.read_parquet(deterministic_root / "output" / "debug" / "audit_wide.parquet")
    verisk = wide.filter(pl.col(AF.VENDOR) == VendorName.VERISK)

    assert verisk.select(AF.UPLIFT_FACTOR).unique().item() == pytest.approx(0.75)
    assert verisk.select(AF.UPLIFT_FACTOR_CAPPED).unique().item() == pytest.approx(0.75)
    assert verisk.select(AF.VK_PROPORTION).unique().item() == pytest.approx(0.5)
    assert verisk.select(AF.RL_PROPORTION).unique().item() == pytest.approx(0.5)

    fanout = pl.read_parquet(deterministic_root / "output" / "HiscoAIR_202601_main.parquet")
    expected = {1: 75.0, 2: 150.0, 3: 225.0}
    for event_id, expected_loss in expected.items():
        row = fanout.filter(pl.col(H.MODEL_EVENT_ID) == event_id)
        assert row.height == 1
        assert row[H.MODEL_GROSS_LOSS][0] == pytest.approx(expected_loss)

    dialsup = pl.read_parquet(deterministic_root / "output" / "HiscoAIR_dialsup.parquet")
    for event_id, expected_loss in {1: 100.0, 2: 200.0, 3: 300.0}.items():
        row = dialsup.filter(pl.col(H.MODEL_EVENT_ID) == event_id)
        assert row.height == 1
        assert row[H.MODEL_GROSS_LOSS][0] == pytest.approx(expected_loss)


def test_cli_derive_blending_reads_fake_ep_summary_files(deterministic_root: Path, tmp_path: Path):
    output = tmp_path / "derived_blending_weights.csv"

    assert cli_main(["derive-blending", "--output", str(output)]) == 0

    derived = pl.read_csv(output)
    rl_weight = derived.filter(
        (pl.col(BW.PERIL_ID) == 1)
        & (pl.col(BW.RETURN_PERIOD) == 0)
        & (pl.col(BW.VENDOR) == VendorName.RISKLINK)
    )[BW.WEIGHT][0]
    vk_weight = derived.filter(
        (pl.col(BW.PERIL_ID) == 1)
        & (pl.col(BW.RETURN_PERIOD) == 0)
        & (pl.col(BW.VENDOR) == VendorName.VERISK)
    )[BW.WEIGHT][0]

    assert rl_weight == pytest.approx(500.0 / 1500.0)
    assert vk_weight == pytest.approx(1000.0 / 1500.0)


def test_run_time_blending_derivation_uses_fake_ep_summary_files(deterministic_root: Path, monkeypatch: pytest.MonkeyPatch):
    from rollup.run_inputs import derive_blending_for_run

    cfg = config.resolve()
    blending = derive_blending_for_run(cfg)

    assert blending.weights is not None
    assert "derived" in blending.message
    assert (deterministic_root / "output" / "debug" / "derived_blending_weights.csv").exists()

    df = blending.weights.collect()
    rl_weight = df.filter(
        (pl.col(BW.PERIL_ID) == 1)
        & (pl.col(BW.RETURN_PERIOD) == 0)
        & (pl.col(BW.VENDOR) == VendorName.RISKLINK)
    )[BW.WEIGHT][0]
    vk_weight = df.filter(
        (pl.col(BW.PERIL_ID) == 1)
        & (pl.col(BW.RETURN_PERIOD) == 0)
        & (pl.col(BW.VENDOR) == VendorName.VERISK)
    )[BW.WEIGHT][0]

    assert rl_weight == pytest.approx(500.0 / 1500.0)
    assert vk_weight == pytest.approx(1000.0 / 1500.0)
