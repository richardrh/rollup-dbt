"""Vendors, config resolution, plan enumeration, interactive confirm."""

from __future__ import annotations

import io
from pathlib import Path

import pytest
import polars as pl

from rollup import config
from rollup.config import EnvVar, VendorName
from rollup.schemas.columns import AnalysesCol as AN
from rollup.schemas.columns import RefForecastFactorsCol as FF
from rollup.schemas.columns import RefLobsCol as LB
from rollup.schemas.columns import ValidAnalysesCol as VA
from rollup.seeds import REQUIRED_SEEDS, SEEDS


# -----------------------------------------------------------------------------
# Vendor + Config resolution
# -----------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    for var in EnvVar:
        monkeypatch.delenv(var, raising=False)


def test_resolve_has_two_vendors():
    cfg = config.resolve()
    assert tuple(v.name for v in cfg.vendors) == (VendorName.VERISK, VendorName.RISKLINK)


def test_verisk_n_simulations_is_10k():
    v = config.resolve().vendor(VendorName.VERISK)
    assert v.n_simulations == 10_000
    assert v.hisco_label == "AIR"
    assert v.ylt_glob == "air_ylt_*.parquet"


def test_risklink_n_simulations_is_100k():
    v = config.resolve().vendor(VendorName.RISKLINK)
    assert v.n_simulations == 100_000
    assert v.hisco_label == "RMS"
    assert v.ylt_glob == "risklink_ylt*.parquet"


def test_paths_default_under_repo_root():
    cfg = config.resolve()
    assert cfg.seeds_dir == config.REPO_ROOT / "data" / "seeds"
    assert cfg.output_dir == config.REPO_ROOT / "data" / "output"
    assert cfg.vendor(VendorName.VERISK).ylt_dir   == config.REPO_ROOT / "data" / "ylt" / VendorName.VERISK
    assert cfg.vendor(VendorName.RISKLINK).ylt_dir == config.REPO_ROOT / "data" / "ylt" / VendorName.RISKLINK


def test_env_overrides_win(monkeypatch, tmp_path):
    monkeypatch.setenv(EnvVar.SEEDS_DIR,     str(tmp_path / "s"))
    monkeypatch.setenv(EnvVar.YLT_VERISK_DIR, str(tmp_path / "v"))
    cfg = config.resolve()
    assert cfg.seeds_dir == (tmp_path / "s").resolve()
    assert cfg.vendor(VendorName.VERISK).ylt_dir == (tmp_path / "v").resolve()


def test_toml_config_supplies_paths_sql_and_run_settings(monkeypatch, tmp_path):
    toml_path = tmp_path / "rollup.local.toml"
    toml_path.write_text(
        """
[run]
min_loss = 2500.0

[sql]
mssql_conn_str = "mssql+pyodbc://server/database?trusted_connection=yes"

[paths]
data_dir = "data-local"
seeds_dir = "seeds-local"
output_dir = "output-local"

[vendors.verisk]
ylt_dir = "ylt/verisk-local"
ylt_glob = "vk_*.parquet"
ep_summary_dir = "ep/verisk-local"

[vendors.risklink]
ylt_dir = "ylt/risklink-local"
ylt_glob = "rl_*.parquet"
ep_summary_dir = "ep/risklink-local"
"""
    )
    monkeypatch.setattr(config, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(config, "LOCAL_TOML_CONFIG", toml_path)
    monkeypatch.setattr(config, "LEGACY_PY_CONFIG", tmp_path / "missing_config.py")

    cfg = config.resolve()

    assert cfg.min_loss == 2500.0
    assert cfg.mssql_conn_str == "mssql+pyodbc://server/database?trusted_connection=yes"
    assert cfg.seeds_dir == (tmp_path / "seeds-local").resolve()
    assert cfg.output_dir == (tmp_path / "output-local").resolve()
    assert cfg.vendor(VendorName.VERISK).ylt_dir == (tmp_path / "ylt/verisk-local").resolve()
    assert cfg.vendor(VendorName.VERISK).ylt_glob == "vk_*.parquet"
    assert cfg.vendor(VendorName.VERISK).ep_summary_dir == (tmp_path / "ep/verisk-local").resolve()
    assert cfg.vendor(VendorName.RISKLINK).ylt_dir == (tmp_path / "ylt/risklink-local").resolve()
    assert cfg.vendor(VendorName.RISKLINK).ylt_glob == "rl_*.parquet"
    assert cfg.vendor(VendorName.RISKLINK).ep_summary_dir == (tmp_path / "ep/risklink-local").resolve()


def test_env_overrides_toml_config(monkeypatch, tmp_path):
    toml_path = tmp_path / "rollup.local.toml"
    toml_path.write_text(
        """
[sql]
mssql_conn_str = "mssql+pyodbc://toml/database"

[paths]
seeds_dir = "toml-seeds"
"""
    )
    monkeypatch.setattr(config, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(config, "LOCAL_TOML_CONFIG", toml_path)
    monkeypatch.setattr(config, "LEGACY_PY_CONFIG", tmp_path / "missing_config.py")
    monkeypatch.setenv(EnvVar.SEEDS_DIR, str(tmp_path / "env-seeds"))
    monkeypatch.setenv(EnvVar.MSSQL_CONN_STR, "mssql+pyodbc://env/database")

    cfg = config.resolve()

    assert cfg.seeds_dir == (tmp_path / "env-seeds").resolve()
    assert cfg.mssql_conn_str == "mssql+pyodbc://env/database"


def test_setup_logging_uses_toml_level(monkeypatch, tmp_path):
    toml_path = tmp_path / "rollup.local.toml"
    toml_path.write_text('[logging]\nlevel = "INFO"\n')
    monkeypatch.setattr(config, "LOCAL_TOML_CONFIG", toml_path)
    monkeypatch.setattr(config, "LEGACY_PY_CONFIG", tmp_path / "missing_config.py")
    captured: dict[str, object] = {}
    monkeypatch.setattr("logging.basicConfig", lambda **kwargs: captured.update(kwargs))

    config.setup_logging()

    assert captured["level"] == "INFO"


def test_malformed_toml_config_raises_systemexit(monkeypatch, tmp_path):
    toml_path = tmp_path / "rollup.local.toml"
    toml_path.write_text("[sql\n")
    monkeypatch.setattr(config, "LOCAL_TOML_CONFIG", toml_path)

    with pytest.raises(SystemExit) as exc_info:
        config.resolve()

    assert "rollup.local.toml has a problem" in str(exc_info.value)


def test_unknown_vendor_raises():
    """Runtime check survives even if a caller bypasses the type system."""
    from typing import cast
    cfg = config.resolve()
    with pytest.raises(KeyError):
        cfg.vendor(cast(VendorName, "katrisk"))


# -----------------------------------------------------------------------------
# Plan enumeration
# -----------------------------------------------------------------------------

def _cfg_with_seeds(tmp_path: Path, populate: bool = True) -> config.Config:
    """Synthesize a config whose seeds_dir has real CSVs (so validation succeeds).

    We copy from the real seeds folder so schema validation passes.
    """
    import shutil
    seeds_dir = tmp_path / "seeds"
    seeds_dir.mkdir()
    if populate:
        real_seeds = config.REPO_ROOT / "data" / "seeds"
        for spec in SEEDS:
            dest = seeds_dir / spec.filename
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(real_seeds / spec.filename, dest)
    return config.Config(
        seeds_dir=seeds_dir,
        output_dir=tmp_path / "out",
        vendors=(
            config.Vendor(VendorName.VERISK,   "AIR", 10_000,
                          tmp_path / "ylt" / VendorName.VERISK,
                          "air_ylt_*.parquet",
                          tmp_path / "ep" / VendorName.VERISK),
            config.Vendor(VendorName.RISKLINK, "RMS", 100_000,
                          tmp_path / "ylt" / VendorName.RISKLINK,
                          "risklink_ylt*.parquet",
                          tmp_path / "ep" / VendorName.RISKLINK),
        ),
    )


def test_build_plan_validates_every_seed_schema(tmp_path):
    """Every seed parses and matches its declared schema. Required seeds that
    are stub-empty in the prod folder are surfaced as not-ok by design — the
    pre-flight blocker. This test confirms there are no PARSE / SCHEMA errors;
    empty-required failures are tested separately."""
    plan = config.build_plan(_cfg_with_seeds(tmp_path))
    seeds = plan.seeds_section
    assert len(seeds.checks) == len(SEEDS)
    schema_failures = [
        (c.label, c.note) for c in seeds.checks
        if not c.ok and "REQUIRED seed is empty" not in c.note
    ]
    assert schema_failures == [], schema_failures


def test_build_plan_blocks_when_required_seed_is_empty(tmp_path):
    """A required seed that is schema-valid but empty is flagged not-ok.

    Without this guard the pipeline would silently produce zero-row Hisco
    parquets. We blank out valid_analyses after copying to simulate the state
    where the user hasn't yet populated that seed.
    """
    cfg = _cfg_with_seeds(tmp_path)
    # Blank valid_analyses to header-only — still schema-valid, but empty
    (cfg.seeds_dir / "business/valid_analyses.csv").write_text("vendor,analysis_id\n")
    plan = config.build_plan(cfg)
    empty_required = {
        c.label for c in plan.seeds_section.checks
        if c.label in REQUIRED_SEEDS and c.rows == 0
    }
    not_ok = {c.label for c in plan.seeds_section.checks if not c.ok}
    assert empty_required.issubset(not_ok), (
        f"required seeds with zero rows that aren't flagged: {empty_required - not_ok}"
    )
    assert not plan.all_seeds_ok


def test_missing_seed_flagged(tmp_path):
    cfg = _cfg_with_seeds(tmp_path)
    (cfg.seeds_dir / "business/lobs.csv").unlink()
    plan = config.build_plan(cfg)
    lobs_check = next(c for c in plan.seeds_section.checks if c.label == "lobs")
    assert not lobs_check.ok
    assert "missing" in lobs_check.note
    assert not plan.all_seeds_ok


def test_seed_schema_drift_flagged(tmp_path):
    """Rename a column in a seed CSV — plan reports 'unexpected=[...]' or 'missing=[...]'."""
    cfg = _cfg_with_seeds(tmp_path)
    bad_file = cfg.seeds_dir / "business/lobs.csv"
    content = bad_file.read_text().splitlines()
    content[0] = content[0].replace("lob_id", "renamed_col")
    bad_file.write_text("\n".join(content))
    plan = config.build_plan(cfg)
    lobs_check = next(c for c in plan.seeds_section.checks if c.label == "lobs")
    assert not lobs_check.ok
    assert "missing=" in lobs_check.note or "unexpected=" in lobs_check.note


def test_ylt_section_lists_globbed_files(tmp_path):
    cfg = _cfg_with_seeds(tmp_path)
    v = cfg.vendor(VendorName.VERISK)
    v.ylt_dir.mkdir(parents=True)
    (v.ylt_dir / "air_ylt_c1.parquet").write_bytes(b"x" * 1024)
    (v.ylt_dir / "air_ylt_c2.parquet").write_bytes(b"x" * 2048)
    (v.ylt_dir / "ignore.txt").write_text("nope")
    plan = config.build_plan(cfg)
    sec = next(s for s in plan.sections if s.title == f"ylt {VendorName.VERISK}")
    labels = [c.label for c in sec.checks]
    assert "air_ylt_c1.parquet" in labels
    assert "air_ylt_c2.parquet" in labels
    assert "ignore.txt" not in labels
    assert "n_simulations=10,000" in sec.header


def test_missing_ylt_directory_flagged(tmp_path):
    plan = config.build_plan(_cfg_with_seeds(tmp_path))
    sec = next(s for s in plan.sections if s.title == f"ylt {VendorName.VERISK}")
    assert all(not c.ok for c in sec.checks)


def test_format_plan_contains_every_section(tmp_path):
    plan = config.build_plan(_cfg_with_seeds(tmp_path))
    text = config.format_plan(plan)
    for title in ("seeds",
                  f"ylt {VendorName.VERISK}",          f"ylt {VendorName.RISKLINK}",
                  f"ep_summaries {VendorName.VERISK}", f"ep_summaries {VendorName.RISKLINK}",
                  "lob_peril_validation",
                  "forecast_factors",
                  "output"):
        assert f"[{title}]" in text
    assert "Seeds:" in text
    assert "YLTs:" in text


def test_forecast_coverage_section_reports_dates(tmp_path):
    plan = config.build_plan(_cfg_with_seeds(tmp_path))
    section = next(s for s in plan.sections if s.title == "forecast_factors")

    date_check = next(c for c in section.checks if c.label == "forecast dates")
    coverage_check = next(c for c in section.checks if c.label == "forecast coverage")

    assert date_check.ok
    assert date_check.rows > 0
    assert coverage_check.ok
    assert "covered" in coverage_check.note or "missing factors" in coverage_check.note


def test_forecast_coverage_section_reports_missing_factors(tmp_path):
    cfg = _cfg_with_seeds(tmp_path)
    ff_path = cfg.seeds_dir / "vor" / "forecast_factors.csv"
    ff = pl.read_csv(ff_path)
    first_date = ff[FF.FORECAST_DATE][0]
    ff.filter(pl.col(FF.FORECAST_DATE) != first_date).write_csv(ff_path)

    plan = config.build_plan(cfg)
    section = next(s for s in plan.sections if s.title == "forecast_factors")
    coverage_check = next(c for c in section.checks if c.label == "forecast coverage")

    assert coverage_check.ok
    assert coverage_check.rows > 0
    assert "missing factors" in coverage_check.note


def test_lob_peril_validation_warns_duplicate_analyses_for_lob_peril(tmp_path):
    cfg = _cfg_with_seeds(tmp_path)
    business = cfg.seeds_dir / "business"
    pl.DataFrame({
        LB.LOB_ID: [1],
        LB.MODELLED_LOB: ["LOB_A_SRC_1"],
        LB.ROLLUP_LOB: ["ROLLUP_A"],
        LB.LOB_TYPE: ["prop"],
        LB.CDS_CAT_CLASS_NAME: ["HIC UK Household"],
        LB.OFFICE: ["UK"],
        LB.CLASS: ["HH"],
        LB.CURRENCY: ["GBP"],
    }).write_csv(business / "lobs.csv")
    pl.DataFrame({
        AN.VENDOR: [VendorName.RISKLINK, VendorName.RISKLINK],
        AN.ANALYSIS_ID: ["101", "102"],
        AN.MODELLED_LABEL: ["A", "B"],
        AN.PERIL_ID: [1, 1],
        AN.LOB_ID: [1, 1],
    }, schema={
        AN.VENDOR: pl.String,
        AN.ANALYSIS_ID: pl.String,
        AN.MODELLED_LABEL: pl.String,
        AN.PERIL_ID: pl.Int64,
        AN.LOB_ID: pl.Int64,
    }).write_csv(business / "analyses.csv")
    pl.DataFrame({
        VA.VENDOR: [VendorName.RISKLINK, VendorName.RISKLINK],
        VA.ANALYSIS_ID: ["101", "102"],
    }).write_csv(business / "valid_analyses.csv")

    plan = config.build_plan(cfg)
    section = next(s for s in plan.sections if s.title == "lob_peril_validation")
    check = section.checks[0]

    assert check.ok
    assert plan.all_lob_peril_ok
    assert "WARNING duplicate valid analyses for LOB/peril" in check.note
    assert "lob=1 peril=1" in check.note


# -----------------------------------------------------------------------------
# Interactive confirm
# -----------------------------------------------------------------------------

def test_confirm_with_assume_yes_skips_prompt(tmp_path, monkeypatch):
    plan = config.build_plan(_cfg_with_seeds(tmp_path))
    monkeypatch.setattr("builtins.input", lambda _: pytest.fail("input() called"))
    assert config.confirm(plan, assume_yes=True, stream=io.StringIO()) is True


def test_confirm_non_interactive_returns_false(tmp_path, monkeypatch):
    plan = config.build_plan(_cfg_with_seeds(tmp_path))
    monkeypatch.setattr("sys.stdin", io.StringIO(""))  # stdin.isatty() → False
    monkeypatch.setattr("builtins.input", lambda _: pytest.fail("input() called"))
    assert config.confirm(plan, assume_yes=False, stream=io.StringIO()) is False


# -----------------------------------------------------------------------------
# Schema-diff reporting: YLT parquets + seed dtype drift
# -----------------------------------------------------------------------------

def test_ylt_section_reports_schema_mismatch(tmp_path):
    """A parquet with a renamed column triggers a schema-diff check failure."""
    import polars as pl
    cfg = _cfg_with_seeds(tmp_path)
    v = cfg.vendor(VendorName.VERISK)
    v.ylt_dir.mkdir(parents=True)
    # Build a parquet whose schema is a wrong shape: rename 'EventID' → 'OOPS'
    bad = pl.DataFrame({
        "Analysis": ["x"], "ExposureAttribute": ["y"], "CatalogTypeCode": ["z"],
        "OOPS": [1], "ModelCode": [1], "YearID": [1], "PerilSetCode": [1],
        "GroundUpLoss": [0.0], "GrossLoss": [0.0], "NetOfPreCatLoss": [0.0],
        "filename": ["f"],
    })
    bad.write_parquet(v.ylt_dir / "air_ylt_c1.parquet")
    plan = config.build_plan(cfg)
    sec = next(s for s in plan.sections if s.title == f"ylt {VendorName.VERISK}")
    chk = next(c for c in sec.checks if c.label == "air_ylt_c1.parquet")
    assert not chk.ok
    assert "missing=" in chk.note and "EventID" in chk.note
    assert "unexpected=" in chk.note and "OOPS" in chk.note


def test_ylt_section_reports_dtype_drift(tmp_path):
    """A parquet column with the wrong dtype is reported as wrong_dtype."""
    import polars as pl
    cfg = _cfg_with_seeds(tmp_path)
    v = cfg.vendor(VendorName.VERISK)
    v.ylt_dir.mkdir(parents=True)
    # GrossLoss should be Float64; write Int64 instead (every other column correct).
    bad = pl.DataFrame({
        "Analysis": ["x"], "ExposureAttribute": ["y"], "CatalogTypeCode": ["z"],
        "EventID": [1], "ModelCode": [1], "YearID": [1], "PerilSetCode": [1],
        "GroundUpLoss": [0.0], "GrossLoss": [0],   # Int64 instead of Float64
        "NetOfPreCatLoss": [0.0], "filename": ["f"],
    })
    bad.write_parquet(v.ylt_dir / "air_ylt_c1.parquet")
    plan = config.build_plan(cfg)
    sec = next(s for s in plan.sections if s.title == f"ylt {VendorName.VERISK}")
    chk = next(c for c in sec.checks if c.label == "air_ylt_c1.parquet")
    assert not chk.ok
    assert "wrong_dtype=" in chk.note
    assert "GrossLoss" in chk.note


def test_seed_section_reports_dtype_drift(tmp_path):
    """A seed CSV whose header matches but dtype is wrong reports wrong_dtype."""
    cfg = _cfg_with_seeds(tmp_path)
    # lobs.csv: lob_id is Int64 in REF_LOBS. Write a value that won't coerce.
    bad = cfg.seeds_dir / "business" / "lobs.csv"
    bad.write_text(
        "lob_id,modelled_lob,rollup_lob,lob_type,cds_cat_class_name,office,class,currency\n"
        "abc,foo,bar,baz,quux,L,X,GBP\n"  # 'abc' isn't an Int64
    )
    plan = config.build_plan(cfg)
    chk = next(c for c in plan.seeds_section.checks if c.label == "lobs")
    assert not chk.ok
    # The note should reference dtype drift OR a parse error mentioning lob_id.
    assert "wrong_dtype" in chk.note or "lob_id" in chk.note
