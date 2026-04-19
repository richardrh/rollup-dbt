"""Typed loaders for every CSV under `polars/seeds/`.

Each loader:
  1. scans the CSV lazily (`pl.scan_csv`) with its `pl.Schema` applied;
  2. validates the resulting LazyFrame against that schema;
  3. returns the LazyFrame so the caller can continue composing queries.

Strict schema validation at the edge means a malformed / out-of-date CSV
fails with a concrete diff instead of a cryptic downstream join error.

The peril dimension is split into four single-purpose tables:

    perils.csv           — one row per rollup peril (peril_id, name, region, peril_family)
    analyses.csv         — (vendor, analysis_id) → peril_id [+ lob_id for RiskLink]
    rollup_scope.csv     — which (lob_id, vendor, analysis_id) triples are official
    blending_weights.csv — long-format (peril_id, sub_peril, vendor, weight)

These replace january's god-table `dim_region_perils` (which mixed peril
display labels, vendor mapping, blending FKs, and per-LOB applies-to flags
into one wide row). One table, one job — the new structure is what the
pipeline consumes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from rollup.schemas import frames as F
from rollup.validate import validate_schema


log = logging.getLogger("rollup.seeds")


@dataclass(frozen=True)
class SeedSpec:
    """File-to-schema mapping. Lets the plan reporter list every seed once."""
    name: str
    filename: str
    schema: pl.Schema


SEEDS: list[SeedSpec] = [
    # ----- LOB + peril dimension (the OPTIMAL split) -----
    SeedSpec("lobs",             "lobs.csv",             F.REF_LOBS),
    SeedSpec("perils",           "perils.csv",           F.PERILS),
    SeedSpec("analyses",         "analyses.csv",         F.ANALYSES),
    SeedSpec("rollup_scope",     "rollup_scope.csv",     F.ROLLUP_SCOPE),
    SeedSpec("blending_weights", "blending_weights.csv", F.BLENDING_WEIGHTS),

    # ----- per-vendor adjustment data -----
    SeedSpec("forecast_factors",    "forecast_factors.csv",    F.REF_FORECAST_FACTORS),
    SeedSpec("fx_rates",            "fx_rates.csv",            F.REF_FX_RATES),
    SeedSpec("euws_rate_factors",   "euws_rate_factors.csv",   F.REF_EUWS_RATE_FACTORS),
    SeedSpec("euws_rank_overrides", "euws_rank_overrides.csv", F.REF_EUWS_RANK_OVERRIDES),

    # ----- event / adjustment reference -----
    SeedSpec("air_events",          "air_events.csv",          F.REF_AIR_EVENTS),
    SeedSpec("fineart_adjustments", "fineart_adjustments.csv", F.REF_FINEART_ADJ),
]


# Seeds that MUST have rows for a real run — empty data here means the
# pipeline will silently produce zero-row Hisco parquets. The plan reporter
# treats an empty REQUIRED seed as a blocker (`Check.ok = False`); other
# seeds may legitimately be empty stubs (e.g. `air_events`, `fineart_*`).
REQUIRED_SEEDS: frozenset[str] = frozenset({
    "lobs",
    "perils",
    "analyses",
    "rollup_scope",        # empty rollup_scope drops every YLT row
    "blending_weights",
    "forecast_factors",    # empty → no forecast tags → no variants → no outputs
    "fx_rates",
    "euws_rate_factors",
    "euws_rank_overrides",
})


def _load(path: Path, schema: pl.Schema, *, name: str) -> pl.LazyFrame:
    if not path.exists():
        raise FileNotFoundError(f"seed '{name}' missing at {path}")
    lf = pl.scan_csv(path, schema=schema)
    validate_schema(lf, schema, name=f"seed.{name}")
    return lf


@dataclass(frozen=True)
class Seeds:
    """All seeds as validated LazyFrames, loaded once at pipeline entry."""
    lobs:                pl.LazyFrame
    perils:              pl.LazyFrame
    analyses:            pl.LazyFrame
    rollup_scope:        pl.LazyFrame
    blending_weights:    pl.LazyFrame
    forecast_factors:    pl.LazyFrame
    fx_rates:            pl.LazyFrame
    euws_rate_factors:   pl.LazyFrame
    euws_rank_overrides: pl.LazyFrame
    air_events:          pl.LazyFrame
    fineart_adjustments: pl.LazyFrame


def load_all(seeds_dir: Path) -> Seeds:
    """Load every seed in `SEEDS` from `seeds_dir`. Fail fast on any gap."""
    log.info(f"loading {len(SEEDS)} seeds from {seeds_dir}")
    return Seeds(**{
        spec.name: _load(seeds_dir / spec.filename, spec.schema, name=spec.name)
        for spec in SEEDS
    })
