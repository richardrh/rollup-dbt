# polars/seeds

Reference / dimension data for the polars rollup pipeline. CSVs follow the
dbt convention: git-friendly, diff-able, one row per natural record, header
in snake_case to match the schema enums in `rollup/schemas/columns.py`.

## What "seed" means here

A seed is a small, versioned reference table that feeds the pipeline. It is
**not** simulation output (YLTs), **not** the EP-summary dumps, **not** any
analyst-provided working file. Those live outside `seeds/` — see the
Pipeline Inputs section of `../README.md` for where.

Each CSV has a corresponding `pl.Schema` in `rollup/schemas/frames.py`.
`rollup/seeds.py` loads each CSV through `pl.scan_csv(..., schema=...)` and
validates at the boundary so shape drift is caught immediately, not in the
middle of a stage ten joins later.

## Shape decisions

january kept some reference tables in a wide shape (e.g. `f_202601`,
`f_202607`, `f_202701` columns on `forecast_factors`; `"Rate to USD"`,
`"Rate to GBP"` columns on `fx_rates`). Adding a new forecast date or
currency required a schema change. We have **reshaped to long format**
where the axis is extensible:

| seed                 | january shape                                    | polars shape                                                   |
| -------------------- | ------------------------------------------------ | -------------------------------------------------------------- |
| `fx_rates`           | wide: `CurrencyCode, "Rate to USD", "Rate to GBP"` | long: `currency_code, target_currency, rate_date, rate`        |
| `forecast_factors`   | wide: `class, office, f_202601, f_202607, f_202701` | long: `class, office, office_iso2, base_date, forecast_date, factor` |
| `blending_factors`   | wide across vendors (`AIRBlend`, `RMSBlend`)     | **unchanged** — shape stays wide because staging uses AIR + RMS weights together in arithmetic; long would force a re-pivot back to wide on every read |
| `euws_rate_factors`  | long already                                     | long (`model_event_id, occ_year, factor`)                      |
| `lobs`               | one row per lob                                  | one row per lob (+ `office`, `class` from january's `lobs_with_class_office` view, precomputed) |

All other seeds are single-dim tables where each row is one natural record;
long vs wide doesn't apply. Those stay in their natural shape.

## Column-naming rules

- `snake_case` headers in every CSV.
- Column names match exactly the string values of `RefXxxCol` StrEnum members
  in `rollup/schemas/columns.py`.
- `id` columns use the table prefix (`lob_id`, `region_peril_id`, …) when it
  makes the column self-describing in joins. Dimension tables where the join
  partner is always explicit keep the bare `id` (e.g. `dim_region_perils.id`).

## Optimal dimensional structure (new)

Replaces january's one god-dimension (`dim_region_perils`) + separate
`dim_risklink_analysis` + wide `blending_factors` with **four single-purpose
tables**. Each table has one job; keys are integers; labels are for display.

```
perils.csv         ← one row per rollup peril (peril_id, name, region, peril_family)
analyses.csv       ← (vendor, analysis_id) → peril_id [+ lob_id for RiskLink]
rollup_scope.csv   ← which (lob_id, peril_id) pairs are in the official rollup
blending_weights.csv ← long-format (peril_id, sub_peril, vendor, weight)
```

Why this beats january:

- No god-dim doing four unrelated jobs in one table.
- `applies_to_{mga, prop, fa}` collapses into `rollup_scope(lob, peril, in_rollup)` — a proper relationship, not flag columns.
- No vendor duplication in the peril dim — vendor lives on `analyses` where it belongs.
- Blending weights long format: adding a vendor is a new row, not a new column.

Why this beats dbt:

- Peril is a first-class entity with an integer id. You cannot typo `"Japan"` vs `"japan"` and break the join — because you're joining on `peril_id = 211`, not on a string.
- Rollup hierarchy is preserved. Rename `"Europe Winter Storm"` → `"EU Windstorm"` and no downstream code changes.
- `rollup_scope` is the single source of truth for "is this (lob, peril) in scope". No overloading with an `is_official` flag that conflates validity and scope.

### Status of the new seeds

| file | rows | source |
|---|---|---|
| `perils.csv`           | 27 | derived from `blending_factors` |
| `blending_weights.csv` | 50 | long-format pivot of `blending_factors` |
| `analyses.csv`         | 7  | Verisk labels from the AIR YLT parquet; `peril_id` mapping is best-effort pending `dim_region_perils` export |
| `rollup_scope.csv`     | 0  | **stub — awaits `applies_to_{mga,prop,fa}` flags from `dim_region_perils`** |

Legacy seeds (`dim_region_perils.csv`, `dim_risklink_analysis.csv`,
`blending_factors.csv`) remain for the current staging code. They will be
retired once the optimal structure is wired through.

## Where the data came from

| seed                         | source (as of 2026-04)                                              | status       |
| ---------------------------- | ------------------------------------------------------------------- | ------------ |
| `lobs.csv`                   | `dbt/seeds/hisco-org/hisco_org__lobs.csv`                           | **populated** (62 rows) |
| `blending_factors.csv`       | `dbt/seeds/vor/vor_blending_factors.csv` (+ `kat_risk_blend = 0`)   | **populated** (30 rows) |
| `euws_rate_factors.csv`      | `dbt/seeds/vor/vor_euws_rate_factors.csv` (header snake_cased)       | **populated** (69 212 rows) |
| `fx_rates.csv`               | **handcrafted** example (3 currencies × 2 targets, 1 date)          | **populated** (6 rows) — replace with real FX snapshot before prod |
| `forecast_factors.csv`       | `dbt/seeds/hisco-org/hisco_org__forecast_factors.csv` (last col renamed) | **populated** (78 rows) |
| `dim_region_perils.csv`      | `loader.main.dim_region_perils` from january's duckdb                | **stub** — header only; export from the duckdb dump before running the pipeline |
| `dim_risklink_analysis.csv`  | `loader.main.dim_rl_analysis`                                        | **stub**     |
| `air_events.csv`             | `loader.reference.air_events`                                        | **stub**     |
| `cds_region_peril.csv`       | `loader.reference.cds_region_peril`                                  | **stub**     |
| `fineart_adjustments.csv`    | `loader.reference.fineart_gross_to_net_adjustment2`                  | **stub**     |
| `flood_rl22_model_events.csv`| `loader.reference.flood_rl22_model_events`                           | **stub**     |

## Re-exporting from january's duckdb

january's table definitions are in
`jan-rollup/duckdb_schema/table_definitions.csv`. To refill any of the six
stubs above, export with:

```sql
COPY (SELECT * FROM loader.reference.air_events) TO 'polars/seeds/air_events.csv'
  WITH (HEADER, DELIMITER ',');
```

Column names and types should already match this folder's schema; if duckdb
exports CamelCase headers (e.g. `EventID`), rename to snake_case before
committing so the loader's strict schema check passes.

## What january did vs what we do now

**january**: every seed was materialised as a duckdb `reference.*` or
`main.dim_*` table. Loading happened via one-off scripts
(`jan-rollup/lib-scripts/`) and the tables persisted in the duckdb file on
disk. Shape drift was silent until a downstream view failed.

**polars**: CSVs live in this folder under git. On pipeline start they are
read via `pl.scan_csv(..., schema=REF_XXX)`. `validate_schema` runs at the
staging boundary and fails fast with a diff on drift. Two-way benefit:
smaller diffs in review, and the reader type-checks the data at the moment
it crosses into the pipeline.
