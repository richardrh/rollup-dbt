"""Typed loaders for every file under `polars/seeds/`.

Each loader:
  1. scans the CSV lazily (`pl.scan_csv`) with its `pl.Schema` applied;
  2. validates the resulting LazyFrame against that schema;
  3. returns the LazyFrame so the caller can continue composing queries.

Strict schema validation at the edge means a malformed / out-of-date CSV
fails with a concrete diff instead of a cryptic downstream join error.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from rollup.schemas import frames as F
from rollup.validate import validate_schema


@dataclass(frozen=True)
class SeedSpec:
    """File-to-schema mapping. Lets the plan reporter list every seed once."""
    name: str
    filename: str
    schema: pl.Schema


SEEDS: list[SeedSpec] = [
    # ----- LOB + peril dimension (optimal structure) -----
    SeedSpec("lobs",                    "lobs.csv",                    F.REF_LOBS),
    SeedSpec("perils",                  "perils.csv",                  F.PERILS),
    SeedSpec("analyses",                "analyses.csv",                F.ANALYSES),
    SeedSpec("rollup_scope",            "rollup_scope.csv",            F.ROLLUP_SCOPE),
    SeedSpec("blending_weights",        "blending_weights.csv",        F.BLENDING_WEIGHTS),

    # ----- per-vendor adjustment data -----
    SeedSpec("forecast_factors",        "forecast_factors.csv",        F.REF_FORECAST_FACTORS),
    SeedSpec("fx_rates",                "fx_rates.csv",                F.REF_FX_RATES),
    SeedSpec("euws_rate_factors",       "euws_rate_factors.csv",       F.REF_EUWS_RATE_FACTORS),

    # ----- event / adjustment reference tables (need data exported from duckdb) -----
    SeedSpec("air_events",              "air_events.csv",              F.REF_AIR_EVENTS),
    SeedSpec("cds_region_peril",        "cds_region_peril.csv",        F.REF_CDS_REGION_PERIL),
    SeedSpec("fineart_adjustments",     "fineart_adjustments.csv",     F.REF_FINEART_ADJ),
    SeedSpec("flood_rl22_model_events", "flood_rl22_model_events.csv", F.REF_FLOOD_RL22),

    # ----- LEGACY (transitional — will retire once staging is rewired to `analyses`/`perils`) -----
    SeedSpec("blending_factors",        "blending_factors.csv",        F.REF_BLENDING_FACTORS),
    SeedSpec("dim_region_perils",       "dim_region_perils.csv",       F.DIM_REGION_PERILS),
    SeedSpec("dim_risklink_analysis",   "dim_risklink_analysis.csv",   F.DIM_RISKLINK_ANALYSIS),
]


def _load(path: Path, schema: pl.Schema, *, name: str) -> pl.LazyFrame:
    if not path.exists():
        raise FileNotFoundError(f"seed '{name}' missing at {path}")
    lf = pl.scan_csv(path, schema=schema)
    validate_schema(lf, schema, name=f"seed.{name}")
    return lf


@dataclass(frozen=True)
class Seeds:
    """All seeds as validated LazyFrames, loaded once at pipeline entry."""
    # optimal structure
    lobs:                    pl.LazyFrame
    perils:                  pl.LazyFrame
    analyses:                pl.LazyFrame
    rollup_scope:            pl.LazyFrame
    blending_weights:        pl.LazyFrame
    # per-vendor adjustment data
    forecast_factors:        pl.LazyFrame
    fx_rates:                pl.LazyFrame
    euws_rate_factors:       pl.LazyFrame
    # event / adjustment reference
    air_events:              pl.LazyFrame
    cds_region_peril:        pl.LazyFrame
    fineart_adjustments:     pl.LazyFrame
    flood_rl22_model_events: pl.LazyFrame
    # legacy
    blending_factors:        pl.LazyFrame
    dim_region_perils:       pl.LazyFrame
    dim_risklink_analysis:   pl.LazyFrame


def load_all(seeds_dir: Path) -> Seeds:
    """Load every seed in `SEEDS` from `seeds_dir`. Fail fast on any gap."""
    return Seeds(**{
        spec.name: _load(seeds_dir / spec.filename, spec.schema, name=spec.name)
        for spec in SEEDS
    })
