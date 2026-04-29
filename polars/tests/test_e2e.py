"""End-to-end: synthetic data in → Hisco parquets out. One test, one run.

Uses `tests/build_test_data.py` to produce an internally-consistent tiny
dataset under `tests/data/`. Points the pipeline's env vars there, calls
`pipeline.run()`, and asserts:
  1. The correct number of Hisco parquets are written.
  2. Every one validates against the `HISCO_FANOUT` schema.
  3. At least one variant has non-empty rows and non-zero ModelGrossLoss
     (i.e. the math actually flowed through — we didn't just produce headers).
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import pytest

from rollup import config
from rollup.config import EnvVar, VendorName
from rollup.pipeline import build_variants, forecast_dates_from_seed, run
from rollup.schemas import frames as F
from rollup.schemas.columns import HiscoFanoutCol as H
from rollup.seeds import load_all

from tests.build_test_data import build as build_test_data


@pytest.fixture(scope="module")
def data_root() -> Path:
    return build_test_data()


@pytest.fixture
def cfg(data_root: Path, monkeypatch) -> config.Config:
    monkeypatch.setenv(EnvVar.SEEDS_DIR,        str(data_root / "seeds"))
    monkeypatch.setenv(EnvVar.DATA_DIR,         str(data_root))
    monkeypatch.setenv(EnvVar.YLT_VERISK_DIR,   str(data_root / "ylt" / VendorName.VERISK))
    monkeypatch.setenv(EnvVar.YLT_RISKLINK_DIR, str(data_root / "ylt" / VendorName.RISKLINK))
    monkeypatch.setenv(EnvVar.OUTPUT_DIR,       str(data_root / "output"))
    return config.resolve()


# -----------------------------------------------------------------------------
# The actual end-to-end run
# -----------------------------------------------------------------------------

def test_pipeline_run_produces_all_hisco_parquets(cfg, data_root):
    output_dir = data_root / "output"
    # Clean output from any prior run so assertions are decisive.
    for p in output_dir.glob("*.parquet"):
        p.unlink()

    run(cfg)

    seeds         = load_all(cfg.seeds_dir)
    variants      = build_variants(forecast_dates_from_seed(seeds), cfg.vendors)
    expected_names = {f"{v.name}.parquet" for v in variants}
    actual_names   = {p.name for p in output_dir.glob("*.parquet")}

    assert actual_names == expected_names, (
        f"output set mismatch.\n"
        f"  missing:  {sorted(expected_names - actual_names)}\n"
        f"  surplus:  {sorted(actual_names - expected_names)}"
    )


def test_every_hisco_parquet_matches_schema(cfg, data_root):
    output_dir = data_root / "output"
    for path in sorted(output_dir.glob("*.parquet")):
        df = pl.read_parquet(path)
        assert df.schema == F.HISCO_FANOUT, (
            f"{path.name} schema mismatch:\n"
            f"  got:  {dict(df.schema)}\n"
            f"  want: {dict(F.HISCO_FANOUT)}"
        )


def test_at_least_one_variant_has_real_numbers(cfg, data_root):
    """Evidence the math ran — not just headers with zero rows."""
    output_dir = data_root / "output"

    total_rows = 0
    nonzero_loss_rows = 0
    for path in output_dir.glob("*.parquet"):
        df = pl.read_parquet(path)
        total_rows += df.height
        nonzero_loss_rows += df.filter(pl.col(H.MODEL_GROSS_LOSS) > 0).height

    assert total_rows > 0, "every Hisco parquet is empty — pipeline ran but produced no rows"
    assert nonzero_loss_rows > 0, (
        "pipeline produced rows but every ModelGrossLoss is zero — factor chain "
        "is zeroing out the loss somewhere"
    )
    print(f"\n[e2e] total rows across all Hisco parquets: {total_rows:,}")
    print(f"[e2e] rows with non-zero ModelGrossLoss:     {nonzero_loss_rows:,}")


def test_variant_count_matches_plan(cfg, data_root):
    """Variant count: MAIN → n_vendors × n_dates; DIALSUP → n_vendors × 1.

    DIALSUP is tag-independent (``loss / rate_to_gbp``) so one file per vendor
    is sufficient. For 2 vendors × 3 dates that gives 6 main + 2 dialsup = 8.
    """
    from rollup.config import Flavor
    seeds    = load_all(cfg.seeds_dir)
    fc_dates = forecast_dates_from_seed(seeds)
    variants = build_variants(fc_dates, cfg.vendors)

    n_main    = sum(f == Flavor.MAIN    for v in cfg.vendors for f in v.flavors) * len(fc_dates)
    n_dialsup = sum(f == Flavor.DIALSUP for v in cfg.vendors for f in v.flavors)
    expected  = n_main + n_dialsup

    assert len(variants) == expected
    print(f"\n[e2e] built {len(variants)} Hisco variants "
          f"({len(cfg.vendors)} vendors × {len(fc_dates)} forecast dates main + "
          f"{n_dialsup} dialsup)")


def test_dump_interim_produces_audit_parquets(cfg, data_root):
    debug_dir = data_root / "output" / "debug"
    for p in debug_dir.glob("*.parquet") if debug_dir.exists() else []:
        p.unlink()

    run(cfg, dump_interim=True)

    wide = pl.read_parquet(debug_dir / "audit_wide.parquet")
    long = pl.read_parquet(debug_dir / "audit_long.parquet")

    # Wide: one row per YLT event; factor chain reads left-to-right.
    assert wide.height > 0
    assert "loss_raw" in wide.columns
    # Factor columns sit next to the metric they produce.
    cols = wide.columns
    assert cols.index("uplift_factor_on_base_model_capped") < cols.index("loss_uplifted_capped")
    assert cols.index("rate_to_gbp")                        < cols.index("loss_uplifted_capped_localccy")
    seeds = load_all(cfg.seeds_dir)
    tags  = [d.strftime("%Y%m") for d in forecast_dates_from_seed(seeds)]
    for y in tags:
        assert cols.index(f"f_{y}") < cols.index(f"loss_uplifted_capped_localccy_{y}")
        assert cols.index("euws_factor") < cols.index(f"loss_uplifted_capped_localccy_{y}_euws")

    # Long: N events × M metrics, all non-null.
    # Metric columns: loss + 3 year-invariant + 3 chain stages × n_tags + 1 dialsup
    assert long["metric_name"].n_unique() >= 1 + 3 + 3 * len(tags) + 1
    assert long.filter(pl.col("value").is_null()).height == 0

    print(f"\n[e2e] audit_wide: {wide.shape}, {len(wide.columns)} cols")
    print(f"[e2e] audit_long: {long.shape}, "
          f"{long['metric_name'].n_unique()} distinct metric_name values")
