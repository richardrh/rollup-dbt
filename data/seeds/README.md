# data/seeds

Reference / dimension data for the polars rollup pipeline. Most seeds are CSVs
that follow the dbt convention: diff-friendly, one row per natural record,
header in snake_case to match the schema enums in
`polars/rollup/schemas/columns.py`. Event catalogues are authoritative parquet
exports projected by the seed loader.

Git-tracked so the pipeline ships with working fixtures, but **user-owned**.
Refresh placeholder files from your source data before a real run. The
pipeline enforces one-job seeds: `data/seeds/` is where reference data
lives; `data/ylt/`, `data/ep_summaries/`, `data/output/` are siblings for
simulation input/output.

## What to do if you need to populate these for a real run

Start at [`../../polars/RH-TODO-DATA.md`](../../polars/RH-TODO-DATA.md) —
a simple "collect these files and put them here" checklist. That
document has the column schemas you need to give your data source.

Each seed has a corresponding `pl.Schema` in
`polars/rollup/schemas/frames.py`. `polars/rollup/seeds.py` loads each CSV or
parquet and validates at the boundary so shape drift is caught immediately,
not in the middle of a stage ten joins later.

## The 11 seeds

```
perils.csv            — one row per rollup peril (peril_id, name, region, peril_family)
analyses.csv          — numeric (vendor, analysis_id) → peril_id [+ lob_id for RiskLink]
valid_analyses.csv    — numeric vendor analysis IDs allowed into this run
blending_weights.csv  — long-format (peril_id, return_period, vendor, base_model, weight)
lobs.csv              — LOB dimension + office + class
forecast_factors.csv  — (class, office, forecast_date) → factor
fx_rates.csv          — long-format (currency_code, target_currency, rate_date, rate)
euws_rate_factors.csv — per-event EUWS factors
euws_rank_overrides.csv — per-LOB rank-threshold overrides for EUWS
verisk_events.parquet — Verisk event catalogue (EventID, ModelID, Event, Year, Day)
risklink_flood22_model_events.parquet — RiskLink event catalogue
```

| seed                       | rows in this branch | populated by |
| -------------------------- | ------------------- | ------------ |
| `lobs.csv`                 | 62                  | dbt          |
| `perils.csv`               | **stub (0)**        | duckdb export — required |
| `analyses.csv`             | **stub (0)**        | duckdb export — required |
| `valid_analyses.csv`       | 12                  | operator-owned allow-list — required |
| `blending_weights.csv`     | **stub (0)**        | duckdb export — required |
| `forecast_factors.csv`     | 78                  | dbt — bump per forecast cycle |
| `fx_rates.csv`             | 6 (handcrafted)     | replace with real FX snapshot before prod |
| `euws_rate_factors.csv`    | 69 212              | dbt          |
| `euws_rank_overrides.csv`  | 1                   | hand-curated |
| `verisk_events.parquet`    | populated           | parquet export from `reference.air_events` |
| `risklink_flood22_model_events.parquet` | populated | parquet export from RiskLink event catalogue |

The full column schemas for the stub seeds (and what happens when they
stay empty) live in [`../../polars/RH-TODO-DATA.md`](../../polars/RH-TODO-DATA.md).

## One table, one job

january kept the peril dimension as a god-table — `dim_region_perils` with
14 columns mixing peril labels, vendor mapping, blending FKs, and per-LOB
applies-to flags. The new seed structure splits that into focused tables that
each have one job:

| split table              | role                                                                  |
| ------------------------ | --------------------------------------------------------------------- |
| `perils.csv`             | the peril dimension itself — `peril_id` is the canonical primary key shared across vendors. `peril_family` ("FL", "WS", …) is available for seed derivation and QA. |
| `analyses.csv`           | numeric `(vendor, analysis_id) → peril_id` lookup. For RiskLink the `lob_id` is also populated (one analysis = one (lob, peril)); for Verisk it's NULL and raw AIR labels live in `modelled_label`. |
| `valid_analyses.csv`     | explicit allow-list of numeric vendor analysis IDs included in the rollup. Bundled Verisk IDs are placeholders. Replaces per-LOB `applies_to_{mga,prop,fa}` filtering. |
| `blending_weights.csv`   | long-format blend weights. Adding a vendor is a new row, not a new column. |

What this beats:

- **No god-dim doing four unrelated jobs in one table.**
- `applies_to_{mga, prop, fa}` collapses into `valid_analyses(vendor, analysis_id)` — a direct operator allow-list.
- No vendor duplication in the peril dim — vendor lives on `analyses` where it belongs.
- Blending weights long format: new vendor or new sub-peril is a new row, not a schema change.
- Base-model selection lives in `blending_weights.base_model`; generated weights default flood perils to RiskLink but operators can override the seed.

## Shape decisions on the other seeds

january kept some reference tables in a wide shape (e.g. `f_202601`,
`f_202607`, `f_202701` columns on `forecast_factors`; `"Rate to USD"`,
`"Rate to GBP"` columns on `fx_rates`). Adding a new forecast date or
currency required a schema change. We have **reshaped to long format**
where the axis is extensible:

| seed                 | january shape                                         | polars shape                                                   |
| -------------------- | ----------------------------------------------------- | -------------------------------------------------------------- |
| `fx_rates`           | wide: `CurrencyCode, "Rate to USD", "Rate to GBP"`    | long: `currency_code, target_currency, rate_date, rate`        |
| `forecast_factors`   | wide: `class, office, f_202601, f_202607, f_202701`   | long: `class, office, office_iso2, forecast_date, factor` |
| `blending_weights`   | wide across vendors (`AIRBlend`, `RMSBlend`, …)       | long: `peril_id, return_period, sub_peril, vendor, base_model, weight` |
| `euws_rate_factors`  | long already                                          | long (`model_event_id, occ_year, factor`)                      |
| `lobs`               | one row per lob                                       | one row per lob (+ `office`, `class` from january's `lobs_with_class_office` view) |

## Column-naming rules

- `snake_case` headers in every CSV.
- Column names match exactly the string values of `RefXxxCol` / `PerilsCol`
  / `AnalysesCol` / etc. StrEnum members in `rollup/schemas/columns.py`.
- `peril_id` is the canonical key (matches january's `dim_region_perils.id`
  integer values). Use `peril_id` in every cross-seed FK; never use the
  derived `rollup_region_peril` string.
