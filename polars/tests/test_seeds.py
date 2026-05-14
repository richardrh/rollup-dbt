"""Each seed under `data/seeds/` loads clean and matches its declared schema."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from rollup import seeds
from rollup.config import CurrencyCode
from rollup.schemas import frames as F
from rollup.schemas.columns import AnalysesCol as AN
from rollup.schemas.columns import RefForecastFactorsCol as FF
from rollup.schemas.columns import RefFxRatesCol as FX
from rollup.schemas.columns import RefLobsCol as LB
from rollup.schemas.columns import ValidAnalysesCol as VA
from rollup.validate import SchemaError


SEEDS_DIR = Path(__file__).resolve().parents[2] / "data" / "seeds"


# -----------------------------------------------------------------------------
# Seed discovery is explicit path resolution, not header-based guessing.
# -----------------------------------------------------------------------------

def test_seed_file_map_matches_schema_registry():
    assert set(seeds.SEED_FILES) == set(seeds.SCHEMA_REGISTRY)


def test_discover_returns_fixed_relative_paths():
    discovered = {spec.name: spec.filename for spec in seeds.discover(SEEDS_DIR)}
    for name, filename in seeds.SEED_FILES.items():
        assert discovered[name] == filename


def test_discover_does_not_guess_by_header(tmp_path):
    """A matching CSV in the wrong folder should not satisfy a seed."""
    loose_dir = tmp_path / "loose"
    loose_dir.mkdir()
    (loose_dir / "lobs_like.csv").write_text(
        ",".join(F.REF_LOBS.names()) + "\n"
    )

    discovered = {spec.name: spec.filename for spec in seeds.discover(tmp_path)}

    assert discovered["lobs"] == ""


# -----------------------------------------------------------------------------
# Every seed exists, scans, and its schema matches the declared pl.Schema.
# -----------------------------------------------------------------------------

@pytest.mark.parametrize("spec", seeds.SEEDS, ids=lambda s: s.name)
def test_seed_file_exists_and_schema_matches(spec):
    path = SEEDS_DIR / spec.filename
    assert path.exists(), f"seed file missing: {path}"
    lf = seeds.load_seed_file(path, spec.schema, name=spec.name)
    assert lf.collect_schema() == spec.schema


# -----------------------------------------------------------------------------
# `load_all` returns a Seeds dataclass; populated seeds have rows.
# -----------------------------------------------------------------------------

def test_load_all_returns_all_lazyframes():
    bundle = seeds.load_all(SEEDS_DIR)
    for spec in seeds.SEEDS:
        lf = getattr(bundle, spec.name)
        assert isinstance(lf, pl.LazyFrame), f"{spec.name} is not a LazyFrame"


def test_populated_seeds_have_rows():
    bundle = seeds.load_all(SEEDS_DIR)
    assert bundle.lobs.collect().height >= 60
    assert bundle.euws_rate_factors.collect().height > 1000
    assert bundle.forecast_factors.collect().height > 50
    assert bundle.fx_rates.collect().height == 6


def test_event_catalogue_seeds_load_from_authoritative_parquets():
    """Event catalogue seeds are parquet exports projected into canonical schema."""
    bundle = seeds.load_all(SEEDS_DIR)
    assert bundle.air_events.collect().height > 0
    assert bundle.risklink_events.collect().height > 0


def test_event_catalogue_csv_fallbacks_are_not_discovered(tmp_path):
    validation = tmp_path / "validation"
    validation.mkdir()
    (validation / "air_events.csv").write_text("event_id,model_id,event,year,day\n")
    (validation / "risklink_events.csv").write_text("event_id,year,day\n")

    discovered = {spec.name: spec.filename for spec in seeds.discover(tmp_path)}

    assert discovered["air_events"] == ""
    assert discovered["risklink_events"] == ""


# -----------------------------------------------------------------------------
# Failure modes
# -----------------------------------------------------------------------------

def test_missing_seed_raises_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError, match="seed 'lobs' missing"):
        seeds.load_all(tmp_path)


def test_drifted_seed_raises_schema_error(tmp_path):
    """If a column is renamed on disk, validate_schema flags it at load time."""
    bad = tmp_path / "lobs.csv"
    bad.write_text("lob_id,modelled_lob\n1,foo\n")
    # Scan bypasses our loader and uses the file's own inferred schema.
    lf = pl.scan_csv(bad)
    with pytest.raises(SchemaError):
        from rollup.validate import validate_schema
        validate_schema(lf, F.REF_LOBS, name="lobs")


# -----------------------------------------------------------------------------
# Long-format shape spot-checks (extensibility was the reason for the reshape)
# -----------------------------------------------------------------------------

def test_fx_rates_is_long_format():
    """One row per (currency_code, target_currency, rate_date). GBP→GBP=1.0."""
    bundle = seeds.load_all(SEEDS_DIR)
    df = bundle.fx_rates.collect()
    assert df.columns == [FX.CURRENCY_CODE, FX.TARGET_CURRENCY, FX.RATE_DATE, FX.RATE]
    # GBP→GBP identity sanity
    gbp = df.filter(
        (pl.col(FX.CURRENCY_CODE)   == CurrencyCode.GBP) &
        (pl.col(FX.TARGET_CURRENCY) == CurrencyCode.GBP)
    )
    assert gbp.height == 1
    assert gbp.row(0, named=True)[FX.RATE] == 1.0


def test_forecast_factors_is_long_format():
    bundle = seeds.load_all(SEEDS_DIR)
    df = bundle.forecast_factors.collect()
    assert FF.FORECAST_DATE in df.columns
    assert FF.FACTOR        in df.columns
    # No wide f_202601-style columns (those are runtime-derived, never in the seed)
    assert not any(c.startswith("f_20") for c in df.columns)


def test_lobs_has_office_and_class():
    bundle = seeds.load_all(SEEDS_DIR)
    df = bundle.lobs.collect()
    assert LB.OFFICE in df.columns
    assert LB.CLASS in df.columns


def test_valid_analyses_uses_verisk_gcadj_wind_labels():
    bundle = seeds.load_all(SEEDS_DIR)
    valid = bundle.valid_analyses.collect()
    analyses = bundle.analyses.collect()

    labels = set(
        analyses
        .join(
            valid,
            left_on=[AN.VENDOR, AN.ANALYSIS_ID],
            right_on=[VA.VENDOR, VA.ANALYSIS_ID],
            how="inner",
        )
        .filter(pl.col(AN.VENDOR) == "verisk")
        [AN.MODELLED_LABEL]
        .to_list()
    )

    assert labels == {"EU_EQ", "EU_FL", "EU_WS_GCAdj", "UK_FL", "UK_WSSS_GCAdj"}
    assert "EU_WS" not in labels
    assert "UK_WSSS" not in labels


def test_valid_risklink_has_one_analysis_per_lob_peril():
    bundle = seeds.load_all(SEEDS_DIR)
    valid = bundle.valid_analyses.collect()
    analyses = bundle.analyses.collect()

    risklink_valid = (
        analyses
        .join(
            valid,
            left_on=[AN.VENDOR, AN.ANALYSIS_ID],
            right_on=[VA.VENDOR, VA.ANALYSIS_ID],
            how="inner",
        )
        .filter(pl.col(AN.VENDOR) == "risklink")
    )
    duplicates = (
        risklink_valid
        .group_by(AN.LOB_ID, AN.PERIL_ID)
        .len()
        .filter(pl.col("len") > 1)
    )

    assert duplicates.height == 0
