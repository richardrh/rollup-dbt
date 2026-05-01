"""Typed loaders for every CSV under `data/seeds/`.

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

## Adding a new seed

1. Declare the column enum in `schemas/columns.py`.
2. Declare the `pl.Schema` in `schemas/frames.py`.
3. Add a `name → schema` row to `SCHEMA_REGISTRY` below.
4. Drop the CSV anywhere under `data/seeds/` — the loader finds it by
   header match. No file path coding required.
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
    "rollup_scope":        F.ROLLUP_SCOPE,
    # vor: vendor blending / FX / forecast
    "blending_weights":    F.BLENDING_WEIGHTS,
    "forecast_factors":    F.REF_FORECAST_FACTORS,
    "fx_rates":            F.REF_FX_RATES,
    "euws_rate_factors":   F.REF_EUWS_RATE_FACTORS,
    # adjustments
    "euws_rank_overrides": F.REF_EUWS_RANK_OVERRIDES,
    "fineart_adjustments": F.REF_FINEART_ADJ,
    # validation: event catalogues (stubs until real data provided)
    "air_events":          F.REF_AIR_EVENTS,
    "risklink_events":     F.REF_RISKLINK_EVENTS,
}


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


# ---------------------------------------------------------------------------
# Discovery — match CSVs under seeds_dir to schemas by header
# ---------------------------------------------------------------------------

# Per-seed near-miss record: the closest CSV under `seeds_dir` that
# overlapped a schema's header but didn't exact-match. Returned alongside
# the SeedSpec list from `discover()` so callers can render a column-level
# diff (e.g. "missing=[lob_id], unexpected=[renamed_col]") instead of a
# generic "missing".
NearMisses = dict[str, tuple[Path, list[str]]]


def discover(seeds_dir: Path) -> tuple[list[SeedSpec], NearMisses]:
    """Walk `seeds_dir` for `*.csv` and match each against `SCHEMA_REGISTRY`
    by exact column-set equality on the header.

    Returns:
        specs:       one `SeedSpec` per logical seed name (registry order);
                     `filename` is empty for any seed missing from disk.
        near_misses: per-seed best-overlap path + header for seeds with no
                     exact match. Empty dict when every seed matched cleanly.

    Multiple exact matches for the same schema raise `ValueError`. Files
    whose headers don't overlap any schema are silently ignored.
    """
    by_name: dict[str, Path] = {}
    candidates: dict[str, list[tuple[int, Path, list[str]]]] = {}

    if seeds_dir.exists():
        for csv in sorted(seeds_dir.rglob("*.csv")):
            try:
                header = pl.scan_csv(csv).collect_schema().names()
            except Exception as e:
                log.warning(f"seed scan failed for {csv}: {e}")
                continue
            header_set = set(header)
            matches = [
                name for name, schema in SCHEMA_REGISTRY.items()
                if set(schema.names()) == header_set
            ]
            if matches:
                if len(matches) > 1:
                    raise ValueError(f"seed {csv} matches multiple schemas: {matches}")
                name = matches[0]
                if name in by_name:
                    log.warning(
                        f"seed {name!r}: duplicate match — keeping {by_name[name]}, "
                        f"ignoring {csv}"
                    )
                    continue
                by_name[name] = csv
                continue

            # Near-miss candidate: file overlaps with at least one schema
            # but not exactly. Score = size of symmetric difference; lower = closer.
            for name, schema in SCHEMA_REGISTRY.items():
                expected = set(schema.names())
                if not (expected & header_set):
                    continue
                score = len(expected ^ header_set)
                candidates.setdefault(name, []).append((score, csv, header))

    near_misses: NearMisses = {}
    for name in SCHEMA_REGISTRY:
        if name in by_name or name not in candidates:
            continue
        best = min(candidates[name], key=lambda t: t[0])
        near_misses[name] = (best[1], best[2])

    specs = [
        SeedSpec(
            name=name,
            filename=(str(by_name[name].relative_to(seeds_dir))
                      if name in by_name else ""),
            schema=schema,
        )
        for name, schema in SCHEMA_REGISTRY.items()
    ]
    return specs, near_misses


# ---------------------------------------------------------------------------
# Module-level convenience: the SEEDS list, computed against the repo's
# default seeds dir at import time. `config.build_plan` and `load_all`
# both call `discover()` directly against their own `seeds_dir` — this
# constant exists for tests that want to enumerate the canonical seed set.
# ---------------------------------------------------------------------------

_DEFAULT_SEEDS_DIR: Path = Path(__file__).resolve().parents[2] / "data" / "seeds"
SEEDS: list[SeedSpec] = discover(_DEFAULT_SEEDS_DIR)[0]


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

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
    fineart_adjustments: pl.LazyFrame
    air_events:          pl.LazyFrame
    risklink_events:     pl.LazyFrame


def load_all(seeds_dir: Path) -> Seeds:
    """Discover every seed under `seeds_dir`, load it, validate, return.

    A seed missing from disk raises `FileNotFoundError` with the seed name —
    the same behaviour as before discovery was introduced.
    """
    specs, _ = discover(seeds_dir)
    log.info(f"loading {len(specs)} seeds from {seeds_dir}")
    loaded: dict[str, pl.LazyFrame] = {}
    for spec in specs:
        if not spec.filename:
            raise FileNotFoundError(f"seed '{spec.name}' missing under {seeds_dir}")
        loaded[spec.name] = _load(seeds_dir / spec.filename, spec.schema, name=spec.name)
    return Seeds(**loaded)
