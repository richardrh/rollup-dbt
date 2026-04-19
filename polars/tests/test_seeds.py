"""Each CSV under `polars/seeds/` loads clean and matches its declared schema."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from rollup import seeds
from rollup.config import CurrencyCode
from rollup.schemas import frames as F
from rollup.schemas.columns import RefForecastFactorsCol as FF
from rollup.schemas.columns import RefFxRatesCol as FX
from rollup.schemas.columns import RefLobsCol as LB
from rollup.validate import SchemaError


SEEDS_DIR = Path(__file__).resolve().parent.parent / "seeds"


# -----------------------------------------------------------------------------
# Every seed exists, scans, and its schema matches the declared pl.Schema.
# -----------------------------------------------------------------------------

@pytest.mark.parametrize("spec", seeds.SEEDS, ids=lambda s: s.name)
def test_seed_file_exists_and_schema_matches(spec):
    path = SEEDS_DIR / spec.filename
    assert path.exists(), f"seed file missing: {path}"
    lf = pl.scan_csv(path, schema=spec.schema)
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


def test_stub_seeds_are_empty_but_valid():
    """Header-only CSVs scan fine and produce zero-row frames.

    These are the seeds the user must populate from duckdb before a real
    production run — see polars/docs/data-requirements.md.
    """
    bundle = seeds.load_all(SEEDS_DIR)
    for name in ("perils", "analyses", "rollup_scope", "blending_weights",
                 "air_events", "fineart_adjustments"):
        assert getattr(bundle, name).collect().height == 0, f"{name} should be stub-empty"


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
