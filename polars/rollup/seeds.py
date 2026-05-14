"""Typed loaders for every seed under `data/seeds/`.

Each loader:
  1. scans the CSV/parquet lazily with its `pl.Schema` applied or projected;
  2. validates the resulting LazyFrame against that schema;
  3. returns the LazyFrame so the caller can continue composing queries.

Strict schema validation at the edge means a malformed / out-of-date CSV
fails with a concrete diff instead of a cryptic downstream join error.

The peril dimension is split into four single-purpose tables:

    perils.csv           — one row per rollup peril (peril_id, name, region, peril_family)
    analyses.csv         — numeric (vendor, analysis_id) → peril_id [+ lob_id for RiskLink]
    valid_analyses.csv   — which numeric vendor analysis IDs are official inputs
    blending_weights.csv — long-format (peril_id, sub_peril, vendor, weight)

These replace january's god-table `dim_region_perils` (which mixed peril
display labels, vendor mapping, blending FKs, and per-LOB applies-to flags
into one wide row). One table, one job — the new structure is what the
pipeline consumes.

## Adding a new seed

1. Declare the column enum in `schemas/columns.py`.
2. Declare the `pl.Schema` in `schemas/frames.py`.
3. Add a `name → schema` row to `SCHEMA_REGISTRY` below.
4. Add the path to `SEED_FILES` below. Seed paths are explicit so the
   loader is predictable and easy to debug.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from rollup.schemas import frames as F
from rollup.schemas.columns import BlendingWeightsCol
from rollup.validate import validate_column_in_enum, validate_schema


log = logging.getLogger("rollup.seeds")


@dataclass(frozen=True)
class SeedSpec:
    """File-to-schema mapping. `filename` is populated by `discover()`."""
    name:     str
    filename: str           # relative to seeds_dir; empty when not on disk
    schema:   pl.Schema


# ---------------------------------------------------------------------------
# Schema registry — single source of truth for which seeds exist
# ---------------------------------------------------------------------------

SCHEMA_REGISTRY: dict[str, pl.Schema] = {
    # business: LOB + peril dimension
    "lobs":                F.REF_LOBS,
    "perils":              F.PERILS,
    "analyses":            F.ANALYSES,
    "valid_analyses":      F.VALID_ANALYSES,
    # vor: vendor blending / FX / forecast
    "blending_weights":    F.BLENDING_WEIGHTS,
    "forecast_factors":    F.REF_FORECAST_FACTORS,
    "fx_rates":            F.REF_FX_RATES,
    "euws_rate_factors":   F.REF_EUWS_RATE_FACTORS,
    # adjustments
    "euws_rank_overrides": F.REF_EUWS_RANK_OVERRIDES,
    # validation: event catalogues (authoritative parquet exports)
    "air_events":          F.REF_AIR_EVENTS,
    "risklink_events":     F.REF_RISKLINK_EVENTS,
}


# Seeds that MUST have rows for a real run — empty data here means the
# pipeline will silently produce zero-row Hisco parquets. The plan reporter
# treats an empty REQUIRED seed as a blocker (`Check.ok = False`); other
# seeds may legitimately be optional/empty reference catalogues.
REQUIRED_SEEDS: frozenset[str] = frozenset({
    "lobs",
    "perils",
    "analyses",
    "valid_analyses",      # empty valid_analyses drops every YLT/EP row
    "blending_weights",
    "forecast_factors",    # empty → no forecast tags → no variants → no outputs
    "fx_rates",
    "euws_rate_factors",
    "euws_rank_overrides",
})


# ---------------------------------------------------------------------------
# Discovery — resolve each logical seed to its explicit path
# ---------------------------------------------------------------------------

SEED_FILES: dict[str, str] = {
    "lobs":                "business/lobs.csv",
    "perils":              "business/perils.csv",
    "analyses":            "business/analyses.csv",
    "valid_analyses":      "business/valid_analyses.csv",
    "blending_weights":    "vor/blending_weights.csv",
    "forecast_factors":    "vor/forecast_factors.csv",
    "fx_rates":            "vor/fx_rates.csv",
    "euws_rate_factors":   "vor/euws_rate_factors.csv",
    "euws_rank_overrides": "adjustments/euws_rank_overrides.csv",
    "air_events":          "validation/verisk_events.parquet",
    "risklink_events":     "validation/risklink_flood22_model_events.parquet",
}

SEED_FILE_CANDIDATES: dict[str, tuple[str, ...]] = {
    name: (filename,) for name, filename in SEED_FILES.items()
}


if set(SEED_FILES) != set(SCHEMA_REGISTRY):
    missing = sorted(set(SCHEMA_REGISTRY) - set(SEED_FILES))
    extra = sorted(set(SEED_FILES) - set(SCHEMA_REGISTRY))
    raise RuntimeError(f"SEED_FILES must match SCHEMA_REGISTRY: missing={missing}, extra={extra}")


def discover(seeds_dir: Path) -> list[SeedSpec]:
    """Return one seed spec per registry entry using fixed relative paths.

    `filename` is empty when the configured file is missing. Header and dtype
    validation happen later in the plan checker / loader against the exact
    expected path, so there is no recursive scan or header-based guessing here.
    """
    specs: list[SeedSpec] = []
    for name, schema in SCHEMA_REGISTRY.items():
        filename = next(
            (candidate for candidate in SEED_FILE_CANDIDATES[name] if (seeds_dir / candidate).exists()),
            "",
        )
        specs.append(SeedSpec(
            name=name,
            filename=filename,
            schema=schema,
        ))
    return specs


# ---------------------------------------------------------------------------
# Module-level convenience: the SEEDS list, computed against the repo's
# default seeds dir at import time. `config.build_plan` and `load_all`
# both call `discover()` directly against their own `seeds_dir` — this
# constant exists for tests that want to enumerate the canonical seed set.
# ---------------------------------------------------------------------------

SEEDS: list[SeedSpec] = [
    SeedSpec(name=name, filename=filename, schema=SCHEMA_REGISTRY[name])
    for name, filename in SEED_FILES.items()
]


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_seed_file(path: Path, schema: pl.Schema, *, name: str) -> pl.LazyFrame:
    if not path.exists():
        raise FileNotFoundError(f"seed '{name}' missing at {path}")
    if path.suffix == ".parquet" and name == "air_events":
        lf = pl.scan_parquet(path).select(
            pl.col("EventID").alias("event_id"),
            pl.col("ModelID").alias("model_id"),
            pl.col("Event").alias("event"),
            pl.col("Year").alias("year"),
            pl.col("Day").alias("day"),
        )
    elif path.suffix == ".parquet" and name == "risklink_events":
        lf = pl.scan_parquet(path).select(
            pl.col("ModelEventID").alias("event_id"),
            pl.col("ModelOccurrenceYear").alias("year"),
            pl.col("ModelOccurrenceDate").dt.ordinal_day().cast(pl.Int64).alias("day"),
        )
    elif path.suffix == ".parquet":
        lf = pl.scan_parquet(path)
    else:
        lf = pl.scan_csv(path, schema=schema)
    validate_schema(lf, schema, name=f"seed.{name}")
    return lf


@dataclass(frozen=True)
class Seeds:
    """All seeds as validated LazyFrames, loaded once at pipeline entry."""
    lobs:                pl.LazyFrame
    perils:              pl.LazyFrame
    analyses:            pl.LazyFrame
    valid_analyses:      pl.LazyFrame
    blending_weights:    pl.LazyFrame
    forecast_factors:    pl.LazyFrame
    fx_rates:            pl.LazyFrame
    euws_rate_factors:   pl.LazyFrame
    euws_rank_overrides: pl.LazyFrame
    air_events:          pl.LazyFrame
    risklink_events:     pl.LazyFrame


def load_all(seeds_dir: Path) -> Seeds:
    """Discover every seed under `seeds_dir`, load it, validate, return.

    A seed missing from disk raises `FileNotFoundError` with the seed name —
    the same behaviour as before discovery was introduced.
    """
    from rollup.config import VendorName

    specs = discover(seeds_dir)
    log.info(f"loading {len(specs)} seeds from {seeds_dir}")
    loaded: dict[str, pl.LazyFrame] = {}
    for spec in specs:
        if not spec.filename:
            raise FileNotFoundError(f"seed '{spec.name}' missing under {seeds_dir}")
        loaded[spec.name] = load_seed_file(seeds_dir / spec.filename, spec.schema, name=spec.name)

    validate_column_in_enum(
        loaded["blending_weights"],
        BlendingWeightsCol.BASE_MODEL,
        {v.value for v in VendorName},
        name="seed.blending_weights",
    )
    return Seeds(**loaded)
