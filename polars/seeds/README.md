# polars/seeds

Reference / dimension data for the polars rollup pipeline. CSVs follow the
dbt convention: git-friendly, diff-able, one row per natural record, header
in snake_case to match the schema enums in `rollup/schemas/columns.py`.

## What "seed" means here

A seed is a small, versioned reference table that feeds the pipeline. It is
**not** simulation output (YLTs), **not** the EP-summary dumps, **not** any
analyst-provided working file. Those live outside `seeds/` — see
[`../../docs/data-requirements.md`](../../docs/data-requirements.md) for the full
contract (schema, source SQL, failure-mode table).

Each CSV has a corresponding `pl.Schema` in `rollup/schemas/frames.py`.
`rollup/seeds.py` loads each CSV through `pl.scan_csv(..., schema=...)` and
validates at the boundary so shape drift is caught immediately, not in the
middle of a stage ten joins later.

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

The full population SQL for each stub seed (and what happens when it stays
empty) lives in
[`../../docs/data-requirements.md`](../../docs/data-requirements.md).

## One table, one job

january kept the peril dimension as a god-table — `dim_region_perils` with
14 columns mixing peril labels, vendor mapping, blending FKs, and per-LOB
applies-to flags. The new seed structure splits that into four tables that
each have one job:

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

| seed                 | january shape                                         | polars shape                                                   |
| -------------------- | ----------------------------------------------------- | -------------------------------------------------------------- |
| `fx_rates`           | wide: `CurrencyCode, "Rate to USD", "Rate to GBP"`    | long: `currency_code, target_currency, rate_date, rate`        |
| `forecast_factors`   | wide: `class, office, f_202601, f_202607, f_202701`   | long: `class, office, office_iso2, base_date, forecast_date, factor` |
| `blending_weights`   | wide across vendors (`AIRBlend`, `RMSBlend`, …)       | long: `peril_id, sub_peril, vendor, weight`                    |
| `euws_rate_factors`  | long already                                          | long (`model_event_id, occ_year, factor`)                      |
| `lobs`               | one row per lob                                       | one row per lob (+ `office`, `class` from january's `lobs_with_class_office` view) |

## Column-naming rules

- `snake_case` headers in every CSV.
- Column names match exactly the string values of `RefXxxCol` / `PerilsCol`
  / `AnalysesCol` / etc. StrEnum members in `rollup/schemas/columns.py`.
- `peril_id` is the canonical key (matches january's `dim_region_perils.id`
  integer values). Use `peril_id` in every cross-seed FK; never use the
  derived `rollup_region_peril` string.
