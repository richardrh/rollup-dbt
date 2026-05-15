"""Dry-run validation for EP-summary-driven selected analyses."""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

import polars as pl

from rollup import config
from rollup.config import Config, Vendor, VendorName
from rollup.pipeline import build_staging
from rollup.plan import Check, Plan, Section, build_plan
from rollup.schemas import frames as F
from rollup.schemas.columns import AnalysesCol as AN
from rollup.schemas.columns import NormalizedYltCol as Y
from rollup.schemas.columns import RawRisklinkYltCol as RLK
from rollup.schemas.columns import RawVeriskYltCol as VK
from rollup.schemas.columns import RefLobsCol as LB
from rollup.schemas.columns import PerilsCol as PR
from rollup.schemas.columns import ValidAnalysesCol as VA
from rollup.seeds import Seeds, load_all
from rollup.wizard import run_wizard


def _write_business_seeds(
    seeds_dir: Path,
    *,
    selected_analysis_id: str = "101",
    selected_vendor: VendorName = VendorName.RISKLINK,
    include_verisk: bool = False,
) -> None:
    business = seeds_dir / "business"
    business.mkdir(parents=True, exist_ok=True)
    (business / "lobs.csv").write_text(
        "lob_id,modelled_lob,rollup_lob,lob_type,cds_cat_class_name,office,class,currency\n"
        "1,LOB_A,ROLLUP_A,prop,Class A,UK,HH,GBP\n"
    )
    (business / "perils.csv").write_text(
        "peril_id,name,region,peril_family\n"
        "2,Europe Flood,EU,FL\n"
    )
    analyses_rows = ["risklink,101,EU FL HD,2,1"]
    if include_verisk:
        analyses_rows.append("verisk,900004,EU_WS_GCAdj,2,")
    (business / "analyses.csv").write_text(
        "vendor,analysis_id,modelled_label,peril_id,lob_id\n"
        + "\n".join(analyses_rows)
        + "\n"
    )
    (business / "valid_analyses.csv").write_text("vendor,analysis_id\nrisklink,101\n")
    (business / "selected_analyses.csv").write_text(
        "vendor,analysis_id,include\n"
        f"{selected_vendor},{selected_analysis_id},true\n"
    )


def _write_non_business_seed_files(seeds_dir: Path) -> None:
    vor = seeds_dir / "vor"
    adjustments = seeds_dir / "adjustments"
    validation = seeds_dir / "validation"
    vor.mkdir(parents=True, exist_ok=True)
    adjustments.mkdir(parents=True, exist_ok=True)
    validation.mkdir(parents=True, exist_ok=True)

    (vor / "blending_weights.csv").write_text(",".join(F.BLENDING_WEIGHTS.names()) + "\n")
    (vor / "forecast_factors.csv").write_text(",".join(F.REF_FORECAST_FACTORS.names()) + "\n")
    (vor / "fx_rates.csv").write_text(",".join(F.REF_FX_RATES.names()) + "\n")
    (vor / "euws_rate_factors.csv").write_text(",".join(F.REF_EUWS_RATE_FACTORS.names()) + "\n")
    (adjustments / "euws_rank_overrides.csv").write_text(",".join(F.REF_EUWS_RANK_OVERRIDES.names()) + "\n")
    pl.DataFrame(schema={
        "EventID": pl.Int64,
        "ModelID": pl.Int64,
        "Event": pl.Int64,
        "Year": pl.Int64,
        "Day": pl.Int64,
    }).write_parquet(validation / "verisk_events.parquet")
    pl.DataFrame(schema={
        "ModelEventID": pl.Int64,
        "ModelOccurrenceYear": pl.Int64,
        "ModelOccurrenceDate": pl.Date,
    }).write_parquet(validation / "risklink_flood22_model_events.parquet")


def _write_ep_csv(ep_dir: Path, *, analysis_id: str = "101", modelled_lob: str = "LOB_A") -> None:
    ep_dir.mkdir(parents=True, exist_ok=True)
    (ep_dir / "risklink.long.csv").write_text(
        "vendor,analysis_id,modelled_lob,modelled_peril,ep_type,return_period,loss\n"
        f"risklink,{analysis_id},{modelled_lob},EU FL HD,AAL,0,1.0\n"
    )


def _write_verisk_ep_csv(ep_dir: Path, *, analysis_id: str = "EU_WS_GCAdj") -> None:
    ep_dir.mkdir(parents=True, exist_ok=True)
    (ep_dir / "verisk.long.csv").write_text(
        "vendor,analysis_id,modelled_lob,modelled_peril,ep_type,return_period,loss\n"
        f"verisk,{analysis_id},LOB_A,{analysis_id},AAL,0,1.0\n"
    )


def _write_risklink_ylt(ylt_dir: Path, *, analysis_id: int = 101) -> None:
    ylt_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        RLK.YEAR_ID: [1],
        RLK.EVENT_ID: [10],
        RLK.P_VALUE: [0.1],
        RLK.ANLS_ID: [analysis_id],
        RLK.MEAN_LOSS: [1.0],
        RLK.STD_DEV: [0.1],
        RLK.EXP_VALUE: [1.0],
        RLK.LOSS: [30.0],
    }, schema=F.RAW_RISKLINK_YLT).write_parquet(ylt_dir / "risklink_ylt_test.parquet")


def _write_verisk_ylt(ylt_dir: Path, *, analysis: str = "EU_WS_GCAdj") -> None:
    ylt_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        VK.ANALYSIS: [analysis],
        VK.EXPOSURE_ATTRIBUTE: ["LOB_A"],
        VK.CATALOG_TYPE_CODE: ["STC"],
        VK.EVENT_ID: [1],
        VK.MODEL_CODE: [41],
        VK.YEAR_ID: [1],
        VK.PERILSET_CODE: [9],
        VK.GROUND_UP_LOSS: [110.0],
        VK.GROSS_LOSS: [100.0],
        VK.NET_PRE_CAT_LOSS: [10.0],
        VK.FILENAME: ["a"],
    }, schema=F.RAW_VERISK_YLT).write_parquet(ylt_dir / "air_ylt_test.parquet")


def _cfg(tmp_path: Path) -> Config:
    return Config(
        seeds_dir=tmp_path / "seeds",
        output_dir=tmp_path / "out",
        vendors=(
            Vendor(
                VendorName.RISKLINK,
                "RMS",
                100_000,
                tmp_path / "ylt" / "risklink",
                "risklink_ylt*.parquet",
                tmp_path / "ep" / "risklink",
            ),
        ),
    )


def _cfg_both(tmp_path: Path) -> Config:
    return Config(
        seeds_dir=tmp_path / "seeds",
        output_dir=tmp_path / "out",
        vendors=(
            Vendor(VendorName.VERISK, "AIR", 10_000, tmp_path / "ylt" / "verisk", "air_ylt*.parquet", tmp_path / "ep" / "verisk"),
            Vendor(VendorName.RISKLINK, "RMS", 100_000, tmp_path / "ylt" / "risklink", "risklink_ylt*.parquet", tmp_path / "ep" / "risklink"),
        ),
    )


def _checks(plan: Plan, title: str):
    return next(section.checks for section in plan.sections if section.title == title)


def test_ep_summary_missing_modelled_lob_fails_dry_run(tmp_path: Path):
    cfg = _cfg(tmp_path)
    _write_business_seeds(cfg.seeds_dir)
    cfg.vendor(VendorName.RISKLINK).ep_summary_dir.mkdir(parents=True)
    (cfg.vendor(VendorName.RISKLINK).ep_summary_dir / "risklink.long.csv").write_text(
        "vendor,analysis_id,lob,modelled_peril,ep_type,return_period,loss\n"
        "risklink,101,LOB_A,EU FL HD,AAL,0,1.0\n"
    )

    plan = build_plan(cfg, require_ep_summaries=True)

    ep_checks = _checks(plan, "ep_summaries risklink")
    assert not ep_checks[0].ok
    assert "modelled_lob" in ep_checks[0].note


def test_invalid_canonical_ep_csv_keeps_vendor_not_ready_even_with_xlsx(tmp_path: Path):
    cfg = _cfg(tmp_path)
    _write_business_seeds(cfg.seeds_dir)
    ep_dir = cfg.vendor(VendorName.RISKLINK).ep_summary_dir
    ep_dir.mkdir(parents=True)
    (ep_dir / "source.xlsx").write_bytes(b"placeholder")
    (ep_dir / "risklink.long.csv").write_text(
        "vendor,analysis_id,lob,modelled_peril,ep_type,return_period,loss\n"
        "risklink,101,LOB_A,EU FL HD,AAL,0,1.0\n"
    )

    plan = build_plan(cfg)

    assert not plan.ep_vendor_ready(VendorName.RISKLINK)
    assert not plan.all_ep_ok


def test_selected_analysis_id_not_present_in_ep_summary(tmp_path: Path):
    cfg = _cfg(tmp_path)
    _write_business_seeds(cfg.seeds_dir)
    _write_ep_csv(cfg.vendor(VendorName.RISKLINK).ep_summary_dir, analysis_id="999")

    plan = build_plan(cfg, require_ep_summaries=True)

    checks = {check.label: check for check in _checks(plan, "selected_analysis_validation")}
    assert not checks["selected analysis IDs in EP summary"].ok
    assert "missing from EP summaries" in checks["selected analysis IDs in EP summary"].note


def test_selected_ep_unknown_modelled_lob_fails(tmp_path: Path):
    cfg = _cfg(tmp_path)
    _write_business_seeds(cfg.seeds_dir)
    _write_ep_csv(cfg.vendor(VendorName.RISKLINK).ep_summary_dir, modelled_lob="UNKNOWN_LOB")

    plan = build_plan(cfg, require_ep_summaries=True)

    checks = {check.label: check for check in _checks(plan, "selected_analysis_validation")}
    assert not checks["selected modelled_lob mapping"].ok
    assert "UNKNOWN_LOB" in checks["selected modelled_lob mapping"].note


def test_selected_ep_analysis_resolves_to_lob_and_peril(tmp_path: Path):
    cfg = _cfg(tmp_path)
    _write_business_seeds(cfg.seeds_dir)
    _write_ep_csv(cfg.vendor(VendorName.RISKLINK).ep_summary_dir)
    _write_risklink_ylt(cfg.vendor(VendorName.RISKLINK).ylt_dir)

    plan = build_plan(cfg, require_ep_summaries=True)

    checks = {check.label: check for check in _checks(plan, "selected_analysis_validation")}
    assert checks["selected analysis IDs in EP summary"].ok
    assert checks["selected modelled_lob mapping"].ok
    assert checks["selected peril resolution"].ok
    assert checks["selected EP scope"].ok
    assert checks["selected EP scope"].rows == 1
    assert checks["risklink selected YLT coverage"].ok


def test_selected_analysis_missing_from_ylt_fails_selected_validation(tmp_path: Path):
    cfg = _cfg(tmp_path)
    _write_business_seeds(cfg.seeds_dir)
    _write_ep_csv(cfg.vendor(VendorName.RISKLINK).ep_summary_dir)
    _write_risklink_ylt(cfg.vendor(VendorName.RISKLINK).ylt_dir, analysis_id=999)

    plan = build_plan(cfg, require_ep_summaries=True)

    checks = {check.label: check for check in _checks(plan, "selected_analysis_validation")}
    assert not checks["risklink selected YLT coverage"].ok
    assert "missing anlsid" in checks["risklink selected YLT coverage"].note
    assert not plan.all_selected_analysis_ok


def test_missing_ep_summary_does_not_suppress_selected_ylt_coverage(tmp_path: Path):
    cfg = _cfg(tmp_path)
    _write_business_seeds(cfg.seeds_dir)
    _write_risklink_ylt(cfg.vendor(VendorName.RISKLINK).ylt_dir, analysis_id=999)

    plan = build_plan(cfg, require_ep_summaries=True)

    checks = {check.label: check for check in _checks(plan, "selected_analysis_validation")}
    assert not checks["selected EP summaries"].ok
    assert not checks["risklink selected YLT coverage"].ok
    assert "missing anlsid" in checks["risklink selected YLT coverage"].note
    assert not plan.all_selected_analysis_ok


def test_verisk_selected_label_resolves_through_modelled_label_and_ylt(tmp_path: Path):
    cfg = _cfg_both(tmp_path)
    _write_business_seeds(
        cfg.seeds_dir,
        selected_vendor=VendorName.VERISK,
        selected_analysis_id="EU_WS_GCAdj",
        include_verisk=True,
    )
    _write_verisk_ep_csv(cfg.vendor(VendorName.VERISK).ep_summary_dir)
    _write_verisk_ylt(cfg.vendor(VendorName.VERISK).ylt_dir)

    plan = build_plan(cfg, require_ep_summaries=False)

    checks = {check.label: check for check in _checks(plan, "selected_analysis_validation")}
    assert checks["selected analysis metadata"].ok
    assert checks["selected analysis IDs in EP summary"].ok
    assert checks["verisk selected YLT coverage"].ok
    assert checks["selected EP scope"].ok


def test_verisk_numeric_placeholder_does_not_resolve_as_selected_label(tmp_path: Path):
    cfg = _cfg_both(tmp_path)
    _write_business_seeds(
        cfg.seeds_dir,
        selected_vendor=VendorName.VERISK,
        selected_analysis_id="900004",
        include_verisk=True,
    )
    _write_verisk_ep_csv(cfg.vendor(VendorName.VERISK).ep_summary_dir, analysis_id="EU_WS_GCAdj")
    _write_verisk_ylt(cfg.vendor(VendorName.VERISK).ylt_dir)

    plan = build_plan(cfg, require_ep_summaries=False)

    checks = {check.label: check for check in _checks(plan, "selected_analysis_validation")}
    assert not checks["selected analysis metadata"].ok
    assert "900004" in checks["selected analysis metadata"].note


def test_valid_analyses_divergence_is_reported_but_selected_scope_is_authoritative(tmp_path: Path):
    cfg = _cfg(tmp_path)
    _write_business_seeds(cfg.seeds_dir)
    (cfg.seeds_dir / "business" / "valid_analyses.csv").write_text("vendor,analysis_id\nrisklink,999\n")
    _write_ep_csv(cfg.vendor(VendorName.RISKLINK).ep_summary_dir)
    _write_risklink_ylt(cfg.vendor(VendorName.RISKLINK).ylt_dir)

    plan = build_plan(cfg, require_ep_summaries=True)

    checks = {check.label: check for check in _checks(plan, "selected_analysis_validation")}
    assert checks["valid_analyses compatibility"].ok
    assert "ignored" in checks["valid_analyses compatibility"].note
    assert checks["selected EP scope"].ok


def test_load_all_ignores_invalid_valid_analyses_when_selected_exists(tmp_path: Path):
    cfg = _cfg(tmp_path)
    _write_business_seeds(cfg.seeds_dir)
    _write_non_business_seed_files(cfg.seeds_dir)
    (cfg.seeds_dir / "business" / "valid_analyses.csv").write_text(
        "vendor,analysis_id,stale_column\nrisklink,999,legacy\n"
    )

    bundle = load_all(cfg.seeds_dir)
    valid = bundle.valid_analyses.collect()

    assert valid.schema == F.VALID_ANALYSES
    assert valid.height == 0


def test_build_staging_uses_selected_analyses_not_valid_analyses(tmp_path: Path):
    cfg = _cfg_both(tmp_path)
    _write_business_seeds(cfg.seeds_dir)
    _write_risklink_ylt(cfg.vendor(VendorName.RISKLINK).ylt_dir, analysis_id=101)
    _write_verisk_ylt(cfg.vendor(VendorName.VERISK).ylt_dir)
    pl.DataFrame({
        RLK.YEAR_ID: [1],
        RLK.EVENT_ID: [99],
        RLK.P_VALUE: [0.1],
        RLK.ANLS_ID: [999],
        RLK.MEAN_LOSS: [1.0],
        RLK.STD_DEV: [0.1],
        RLK.EXP_VALUE: [1.0],
        RLK.LOSS: [999.0],
    }, schema=F.RAW_RISKLINK_YLT).write_parquet(cfg.vendor(VendorName.RISKLINK).ylt_dir / "risklink_ylt_extra.parquet")

    seeds = Seeds(
        lobs=pl.DataFrame({
            LB.LOB_ID: [1], LB.MODELLED_LOB: ["LOB_A"], LB.ROLLUP_LOB: ["ROLLUP_A"],
            LB.LOB_TYPE: ["prop"], LB.CDS_CAT_CLASS_NAME: ["Class A"], LB.OFFICE: ["UK"],
            LB.CLASS: ["HH"], LB.CURRENCY: ["GBP"],
        }, schema=F.REF_LOBS).lazy(),
        perils=pl.DataFrame({
            PR.PERIL_ID: [2], PR.NAME: ["Europe Flood"], PR.REGION: ["EU"], PR.PERIL_FAMILY: ["FL"],
        }, schema=F.PERILS).lazy(),
        analyses=pl.DataFrame({
            AN.VENDOR: [VendorName.RISKLINK, VendorName.RISKLINK],
            AN.ANALYSIS_ID: ["101", "999"],
            AN.MODELLED_LABEL: ["EU FL HD", "EU FL HD"],
            AN.PERIL_ID: [2, 2],
            AN.LOB_ID: [1, 1],
        }, schema=F.ANALYSES).lazy(),
        valid_analyses=pl.DataFrame({
            VA.VENDOR: [VendorName.RISKLINK],
            VA.ANALYSIS_ID: ["999"],
        }, schema=F.VALID_ANALYSES).lazy(),
        blending_weights=pl.DataFrame(schema=F.BLENDING_WEIGHTS).lazy(),
        forecast_factors=pl.DataFrame(schema=F.REF_FORECAST_FACTORS).lazy(),
        fx_rates=pl.DataFrame(schema=F.REF_FX_RATES).lazy(),
        euws_rate_factors=pl.DataFrame(schema=F.REF_EUWS_RATE_FACTORS).lazy(),
        euws_rank_overrides=pl.DataFrame(schema=F.REF_EUWS_RANK_OVERRIDES).lazy(),
        air_events=pl.DataFrame(schema=F.REF_AIR_EVENTS).lazy(),
        risklink_events=pl.DataFrame(schema=F.REF_RISKLINK_EVENTS).lazy(),
    )

    staging = build_staging(cfg, seeds).ylt.collect()

    assert staging[Y.EVENT_ID].to_list() == [10]


def test_dry_run_returns_nonzero_when_selected_validation_fails(tmp_path: Path, monkeypatch):
    cfg = _cfg(tmp_path)
    plan = Plan(config=cfg, sections=[
        Section("seeds", "seed-dir", [Check("seed", cfg.seeds_dir, True)]),
        Section(f"ylt {VendorName.RISKLINK}", "ylt-dir", [Check("ylt", cfg.output_dir, True)]),
        Section("selected_analysis_validation", "selected", [Check("selected analysis IDs in EP summary", cfg.seeds_dir, False)]),
        Section("lob_peril_validation", "valid", [Check("one analysis per lob/peril", cfg.seeds_dir, True)]),
    ])
    monkeypatch.setattr(config, "resolve", lambda: cfg)
    monkeypatch.setattr(config, "build_plan", lambda *_args, **_kwargs: plan)

    rc = run_wizard(Namespace(min_loss=None, dry_run=True, yes=False, dump_interim=True))

    assert rc == 2


def test_non_dry_run_aborts_when_selected_validation_fails(tmp_path: Path, monkeypatch):
    cfg = _cfg(tmp_path)
    plan = Plan(config=cfg, sections=[
        Section("seeds", "seed-dir", [Check("seed", cfg.seeds_dir, True)]),
        Section(f"ylt {VendorName.RISKLINK}", "ylt-dir", [Check("ylt", cfg.output_dir, True)]),
        Section("selected_analysis_validation", "selected", [Check("selected modelled_lob mapping", cfg.seeds_dir, False)]),
        Section("lob_peril_validation", "valid", [Check("one analysis per lob/peril", cfg.seeds_dir, True)]),
    ])
    monkeypatch.setattr(config, "resolve", lambda: cfg)
    monkeypatch.setattr(config, "build_plan", lambda *_args, **_kwargs: plan)

    with patch("rollup.pipeline.run") as run:
        rc = run_wizard(Namespace(min_loss=None, dry_run=False, yes=True, dump_interim=True))

    assert rc == 2
    run.assert_not_called()
