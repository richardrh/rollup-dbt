# RH — data collection TODO

What you need to collect before the pipeline can produce real Hisco output.
**No commands to run** — just collect the files and put them where this
document says. I'll run the pipeline for you once everything is in place.

Everything the user owns lives under `data/`. You don't touch the `polars/`
folder — that's source code.

## Target layout

```
<repo>/
└── data/
    ├── seeds/         ← 11 reference CSVs (4 blockers + 7 already-shipped)
    ├── ylt/
    │   ├── verisk/    ← Verisk AIR YLT parquet files go here
    │   └── risklink/  ← RiskLink YLT parquet files go here
    └── output/        ← pipeline writes Hisco parquets here (don't touch)
```

---

# Part 1 — Seed CSVs

Go to `data/seeds/`. Seven files are already there (no action needed):

| file | what it is |
| ---- | ---------- |
| `lobs.csv` | 62 LOB definitions. Already populated from dbt. |
| `forecast_factors.csv` | 78 forecast-factor rows. Already from dbt. Refresh each cycle. |
| `fx_rates.csv` | FX rates. 6 hand-crafted rows — **replace before any real run** (see §5 below). |
| `euws_rate_factors.csv` | 69k EUWS factors. From dbt. |
| `euws_rank_overrides.csv` | Per-LOB EUWS rank overrides. Hand-curated. |
| `air_events.csv` | Verisk event catalogue. Stub — populate if you want to silence a warning. |
| `fineart_adjustments.csv` | Fine-art gross-to-net adjustment. Stub — populate if you model fine-art. |

The **four blocker files** below are currently empty stubs. The pipeline will
refuse to run until they have data.

Hand this section to whoever exports data from the january duckdb
(or wherever the reference data actually lives).

---

## 1. `data/seeds/perils.csv` — the peril dimension

One row per peril. Integer `peril_id` is the canonical key used everywhere.

| column | type | example | notes |
| ------ | ---- | ------- | ----- |
| `peril_id` | integer | `206` | must be unique — one row per peril. Use the existing integer IDs from january's `dim_region_perils.blending_factor_region_peril_id`. |
| `name` | string | `Europe Winter Storm` | display label. |
| `region` | string | `EU` | `EU`, `UK`, `US`, `AU`, etc. |
| `peril_family` | string | `WS` | one of `WS`, `FL`, `EQ`, `TC`, `CS`, `WF`. **Case-sensitive. Flood perils MUST be `"FL"` (upper-case).** |

**Acceptance**: unique `peril_id`, and every flood peril has `peril_family = "FL"`.

---

## 2. `data/seeds/analyses.csv` — vendor analysis → peril (+ lob for RiskLink)

Maps vendor-specific analysis labels to the canonical `peril_id`.

| column | type | example | notes |
| ------ | ---- | ------- | ----- |
| `vendor` | string | `verisk` or `risklink` | only these two values. Lowercase. |
| `analysis_id` | string | Verisk: `EU_WS` / RiskLink: `"501"` | for Verisk = the modelled label; for RiskLink = the `rl_analysis_id` as a string (not integer). |
| `modelled_label` | string | `EU_WS` | human-readable — used downstream to join to `rollup_scope`. For Verisk usually same as `analysis_id`; for RiskLink, the peril family label. |
| `peril_id` | integer | `206` | FK into `perils.csv`. |
| `lob_id` | integer (nullable) | `3` or empty | **Verisk rows: leave empty (NULL). RiskLink rows: populate** with the LOB id from `lobs.csv` — one RiskLink analysis maps to one (lob, peril). |

**Acceptance**: every verisk row has empty `lob_id`; every risklink row has `lob_id` populated.

---

## 3. `data/seeds/rollup_scope.csv` — which (lob, vendor, analysis) triples are in the official rollup

Replaces january's `applies_to_{mga,prop,fa}` flag column. One row per combination that could appear in a YLT.

| column | type | example | notes |
| ------ | ---- | ------- | ----- |
| `lob_id` | integer | `3` | FK into `lobs.csv`. |
| `vendor` | string | `verisk` or `risklink` | lowercase. |
| `analysis_id` | string | `EU_WS` or `UK_WSSS_GCAdj` | the **modelled label** (matches `analyses.modelled_label`), NOT the RiskLink integer. Two analyses can share a `peril_id` (e.g. `UK_WSSS` and `UK_WSSS_GCAdj` both peril 206, but only one is official per LOB) — the granularity is `analysis_id` for exactly this reason. |
| `in_rollup` | boolean | `true` / `false` | `true` = keep, `false` = drop from the pipeline. |

**How to populate from january**: this is the `applies_to_*` CASE from
`dim_region_perils`. Looking at the january SQL:

```
CASE lobs.lob_type
  WHEN 'mga'  THEN dim_region_perils.applies_to_mga
  WHEN 'prop' THEN dim_region_perils.applies_to_prop
  WHEN 'fa'   THEN dim_region_perils.applies_to_fa
END → in_rollup
```

across the cross-product of all LOBs × all `dim_region_perils` rows.

**Acceptance**: at least one row per (lob_id, vendor) has `in_rollup=true`.
If every row is `false` the pipeline will return zero rows.

---

## 4. `data/seeds/blending_weights.csv` — vendor blend weights per peril

Long-format replacement for january's wide `air_blend` / `rms_blend` columns.

| column | type | example | notes |
| ------ | ---- | ------- | ----- |
| `peril_id` | integer | `216` | FK into `perils.csv`. |
| `peril_name` | string | `Europe Flood` | display only (mirrors `perils.name`) — pipeline never joins on this but the CSV is unreadable without it. Empty string is fine but populate if you can. |
| `description` | string | `default 50/50 blend` | free-text reason this row exists. Empty string fine. |
| `sub_peril` | string (nullable) | `216a` or empty | for sub-peril splits (e.g. EU Flood country breakout). Leave empty for the unconditional weight per peril. |
| `vendor` | string | `verisk` or `risklink` | lowercase. Other vendors are silently ignored. |
| `weight` | float | `0.5` | 0..1. Typically vendor weights per peril sum to 1.0. |

**Acceptance**: every peril that can appear in the YLT has at least one `verisk` row AND one `risklink` row.

---

# Part 2 — YLT parquet files

## Verisk (AIR) YLT

**Put files at**: `data/ylt/verisk/air_ylt_*.parquet`

Multiple chunk files are fine — the pipeline globs them as one table.

Required columns (AIR Touchstone export — **CamelCase preserved**):

| column | type | notes |
| ------ | ---- | ----- |
| `Analysis` | string | e.g. `EU_WS` — joined to `analyses.analysis_id` (verisk rows). |
| `ExposureAttribute` | string | the LOB on this row, e.g. `HIC_HH_UK` — joined to `lobs.modelled_lob`. |
| `CatalogTypeCode` | string | filtered to `'STC'` (rest are dropped). |
| `EventID` | integer | event identifier. |
| `ModelCode` | integer | passes through to Hisco. |
| `YearID` | integer | simulation year (1..10 000). |
| `PerilSetCode` | integer | not used; validated for shape. |
| `GroundUpLoss` | float | not used. |
| `GrossLoss` | float | not used. |
| `NetOfPreCatLoss` | float | **this is the loss column the pipeline uses**. |
| `filename` | string | passthrough. |

~10 000 simulation years is expected. If yours differs, let me know so I can bump `Vendor.n_simulations`.

## RiskLink (RMS) YLT

**Put files at**: `data/ylt/risklink/risklink_ylt_*.parquet`

Required columns (RiskLink export — **lowercase**):

| column | type | notes |
| ------ | ---- | ----- |
| `SimulationSetId` | integer | passthrough. |
| `yearid` | integer | simulation year. |
| `eventid` | integer | event identifier. |
| `date` | string | passthrough. |
| `p_value` | float | passthrough. |
| `anlsid` | integer | analysis id. Pipeline casts to string and joins to `analyses.analysis_id` (risklink rows) — so make sure each `anlsid` has a matching RiskLink row in `analyses.csv`. |
| `name` | string | passthrough. |
| `description` | string | passthrough. |
| `rate` | float | passthrough. |
| `meanloss` | float | passthrough. |
| `stddev` | float | passthrough. |
| `expvalue` | float | passthrough. |
| `loss` | float | **this is the loss column the pipeline uses**. |

~100 000 simulation years expected.

---

# Part 3 — Misc

## 5. Replace the placeholder FX file

`data/seeds/fx_rates.csv` currently has 6 hand-crafted rows. Replace with a
real snapshot.

| column | type | example | notes |
| ------ | ---- | ------- | ----- |
| `currency_code` | string | `GBP`, `EUR` | source currency. |
| `target_currency` | string | `GBP` | pipeline filters to `target_currency = 'GBP'`. Every row should have `GBP` here. |
| `rate_date` | date (YYYY-MM-DD) | `2026-01-01` | snapshot date. |
| `rate` | float | `0.88` | `1 currency_code = rate × target_currency`. So `EUR → GBP: 0.88` means 1 EUR = 0.88 GBP. |

**Required entries**: at minimum you need `GBP → GBP = 1.0` and `EUR → GBP = <rate>`. If any LOB's `cds_cat_class_name` contains ` UK ` it derives GBP; if it contains ` EU ` it derives EUR. The pipeline aborts with `MissingFxRateError` if a row needs a currency you didn't supply.

## 6. (OPTIONAL) `data/seeds/air_events.csv`

Silences an orphan-check warning and will be used for `ModelEventDay` in the AIR fan-out later.

| column | type |
| ------ | ---- |
| `event_id` | integer |
| `model_id` | integer |
| `event` | integer |
| `year` | integer |
| `day` | integer |

Leave as stub if you don't have it — pipeline still runs, just logs a warning.

## 7. (OPTIONAL) `data/seeds/fineart_adjustments.csv`

Applies fine-art gross-to-net adjustment. Without this, fine-art LOBs flow through unadjusted.

| column | type |
| ------ | ---- |
| `lob_id` | integer |
| `region_peril_id` | integer |
| `applies_to_fa` | integer (0 or 1) |
| `rollup_region_peril` | string |
| `aal_factor` | float |
| `tail_factor` | float |

---

# Part 4 — Tell me when done

Once every item above is in place, ping me. I'll:

1. Check each file shape matches the spec.
2. Run the pipeline.
3. Send you the 12 Hisco parquets (2 vendors × N forecast dates × 2 flavours) + an audit dump you can sanity-check row by row in excel.

If a file is in the wrong shape, I'll come back with the specific diff.

---

## Column-value gotchas that usually bite

1. **`peril_family` case**: must be exactly `"FL"` for flood. Not `"Flood"`, not `"fl "`, not `"FL "`.
2. **`vendor` case**: must be lowercase `verisk` / `risklink`. Not `Verisk` or `RL`.
3. **`rollup_scope.analysis_id`**: must match the `modelled_label` from `analyses.csv` (e.g. `"EU_WS"`), NOT the raw RiskLink integer (not `"501"`).
4. **`analyses.lob_id`**: NULL/empty for verisk rows, populated for risklink rows. If you swap this, RiskLink joins silently drop everything.
5. **FX coverage**: every currency that could fall out of the `cds_cat_class_name` substring match must have a `target_currency='GBP'` row in `fx_rates.csv`.
6. **office strings**: `forecast_factors.office` must match `lobs.office` exactly (case + spacing). A mismatch silently degrades the forecast factor to 1.0 for that LOB.
