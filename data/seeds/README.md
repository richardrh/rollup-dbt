# data/seeds

Reference / dimension data for the polars rollup pipeline. CSVs follow
the dbt convention: diff-friendly, one row per natural record, header in
snake_case to match the schema enums in `polars/rollup/schemas/columns.py`.

Git-tracked so the pipeline ships with working fixtures, but **user-owned**.
Refresh the stub files from your source data before a real run. The
pipeline enforces one-job seeds: `data/seeds/` is where reference data
lives; `data/ylt/`, `data/ep_summaries/`, `data/output/` are siblings for
simulation input/output.

## What to do if you need to populate these for a real run

Start at [`../../polars/RH-TODO-DATA.md`](../../polars/RH-TODO-DATA.md) —
a simple "collect these files and put them here" checklist. That
document has the column schemas you need to give your data source.

Each CSV has a corresponding `pl.Schema` in
`polars/rollup/schemas/frames.py`. `polars/rollup/seeds.py` loads each
through `pl.scan_csv(..., schema=...)` and validates at the boundary so
shape drift is caught immediately, not in the middle of a stage ten
joins later.

## The 11 seeds

```
perils.csv            — one row per rollup peril (peril_id, name, region, peril_family)
analyses.csv          — (vendor, analysis_id) → peril_id [+ lob_id for RiskLink]
rollup_scope.csv      — which (lob_id, vendor, analysis_id) triples are in scope
blending_weights.csv  — long-format (peril_id, sub_peril, vendor, weight)
lobs.csv              — LOB dimension + office + class
forecast_factors.csv  — (class, office, forecast_date) → factor
fx_rates.csv          — long-format (currency_code, target_currency, rate_date, rate)
euws_rate_factors.csv — per-event EUWS factors
euws_rank_overrides.csv — per-LOB rank-threshold overrides for EUWS
air_events.csv        — Verisk event catalogue (event_id, model_id, year, day)
fineart_adjustments.csv — fine-art gross-to-net AAL factor (optional)
```

| seed                       | rows in this branch | populated by |
| -------------------------- | ------------------- | ------------ |
| `lobs.csv`                 | 62                  | dbt          |
| `perils.csv`               | **stub (0)**        | duckdb export — required |
| `analyses.csv`             | **stub (0)**        | duckdb export — required |
| `rollup_scope.csv`         | **stub (0)**        | duckdb export — required (else pipeline drops every row) |
| `blending_weights.csv`     | **stub (0)**        | duckdb export — required |
| `forecast_factors.csv`     | 78                  | dbt — bump per forecast cycle |
| `fx_rates.csv`             | 6 (handcrafted)     | replace with real FX snapshot before prod |
| `euws_rate_factors.csv`    | 69 212              | dbt          |
| `euws_rank_overrides.csv`  | 1                   | hand-curated |
| `air_events.csv`           | **stub (0)**        | duckdb export — recommended |
| `fineart_adjustments.csv`  | **stub (0)**        | duckdb export — optional |

The full column schemas for the stub seeds (and what happens when they
stay empty) live in [`../../polars/RH-TODO-DATA.md`](../../polars/RH-TODO-DATA.md).

## One table, one job

The peril dimension is split into four tables, each with one job:

| split table              | role                                                                  |
| ------------------------ | --------------------------------------------------------------------- |
| `perils.csv`             | the peril dimension itself — `peril_id` is the canonical primary key shared across vendors. `peril_family` ("FL", "WS", …) is the semantic category used by the flood-base-model rule. |
| `analyses.csv`           | `(vendor, analysis_id) → peril_id` lookup. For RiskLink the `lob_id` is also populated (one analysis = one (lob, peril)); for Verisk it's NULL (lob lives on the YLT row). |
| `rollup_scope.csv`       | which `(lob_id, vendor, analysis_id)` triples are officially in the rollup. Replaces the `applies_to_{mga,prop,fa}` flag fan-out. |
| `blending_weights.csv`   | long-format blend weights. Adding a vendor is a new row, not a new column. |

What this beats:

- **No god-dim doing four unrelated jobs in one table.**
- `applies_to_{mga, prop, fa}` collapses into `rollup_scope(lob, vendor, analysis, in_rollup)` — a real relationship, not flag columns conditioned on `lob_type`.
- No vendor duplication in the peril dim — vendor lives on `analyses` where it belongs.
- Blending weights long format: new vendor or new sub-peril is a new row, not a schema change.
- The flood-base-model rule keys on `peril_family == "FL"` (semantic), not on a substring match against derived strings like `"EU_FL"` / `"UK_FL"`. New flood region in `perils.csv` → no code change.

## Shape decisions on the other seeds

january kept some reference tables in a wide shape (e.g. `f_202601`,
`f_202607`, `f_202701` columns on `forecast_factors`; `"Rate to USD"`,
`"Rate to GBP"` columns on `fx_rates`). Adding a new forecast date or
currency required a schema change. We have **reshaped to long format**
where the axis is extensible:

| seed                 | format |
| -------------------- | ------ |
| `fx_rates`           | long: `currency_code, target_currency, rate_date, rate`        |
| `forecast_factors`   | long: `class, office, office_iso2, forecast_date, factor` |
| `blending_weights`   | long: `peril_id, sub_peril, vendor, weight`                    |
| `euws_rate_factors`  | long: `model_event_id, occ_year, factor`                      |
| `lobs`               | one row per lob (+ `office`, `class` columns) |

## Column-naming rules

- `snake_case` headers in every CSV.
- Column names match exactly the string values of `RefXxxCol` / `PerilsCol`
  / `AnalysesCol` / etc. StrEnum members in `rollup/schemas/columns.py`.
- `peril_id` is the canonical key. Use `peril_id` in every cross-seed FK;
  never use string identifiers like `rollup_region_peril` for joins.
