# File-format reference

Quick reference for every file the pipeline reads. Each section lists the
**path**, **format**, **columns** (with dtype), and **notes**. For the
deeper *why* and the SQL recipes for populating the seeds, see
[`data-requirements.md`](data-requirements.md).

The pre-flight check (`uv run rollup --dry-run`) validates
every file listed here against its declared schema and reports any drift
with `filename | column | reason`.

---

## YLT parquets

### `data/ylt/verisk/air_ylt_*.parquet`

Multi-chunk parquet (`air_ylt_c1.parquet`, `air_ylt_c2.parquet`, …) scanned
as one lazy table. **CamelCase preserved** to match AIR Touchstone export.

| column              | dtype   | notes |
|---------------------|---------|-------|
| `Analysis`          | String  | label e.g. `EU_WS`; joins to `analyses.analysis_id` (vendor='verisk'). |
| `ExposureAttribute` | String  | the LOB on this row, e.g. `HIC_HH_UK`; joins to `lobs.modelled_lob`. |
| `CatalogTypeCode`   | String  | filtered to `'STC'` at staging. |
| `EventID`           | Int64   | event identifier; used for the EUWS join. |
| `ModelCode`         | Int64   | passed through to Hisco. |
| `YearID`            | Int64   | simulation year, 1..n_simulations. |
| `PerilSetCode`      | Int64   | not used; validated for shape. |
| `GroundUpLoss`      | Float64 | not used; `NetOfPreCatLoss` is the loss column. |
| `GrossLoss`         | Float64 | not used. |
| `NetOfPreCatLoss`   | Float64 | **the loss carried into the chain**. |
| `filename`          | String  | passthrough. |

### `data/ylt/risklink/risklink_ylt_*.parquet`

**One row per (yearid, eventid, anlsid)** — *not* a per-period summary.
Filter to `PERSPCODE='RL'` (ground-up loss) before exporting.

| column            | dtype   | notes |
|-------------------|---------|-------|
| `SimulationSetId` | Int64   | passthrough. |
| `yearid`          | Int64   | simulation year. |
| `eventid`         | Int64   | event identifier. |
| `date`            | String  | `YYYY-MM-DD`. |
| `p_value`         | Float64 | passthrough. |
| `anlsid`          | Int64   | analysis id; cast to String and joined to `analyses.analysis_id` (vendor='risklink'). |
| `name`            | String  | passthrough (e.g. `GB FL HD`). |
| `description`     | String  | passthrough. |
| `rate`            | Float64 | passthrough. |
| `meanloss`        | Float64 | passthrough. |
| `stddev`          | Float64 | passthrough. |
| `expvalue`        | Float64 | passthrough. |
| `loss`            | Float64 | **the loss carried into the chain**. |

> Per-event YLTs are only strictly required for `peril_family='FL'` analyses
> (where the base model is RiskLink). For non-flood perils, see
> [data-requirements.md → "Which RiskLink analyses do you actually need?"](data-requirements.md#which-risklink-analyses-do-you-actually-need-to-export).

---

## Seeds — `data/seeds/**/*.csv`

The pipeline auto-discovers seed CSVs by header match — file location
under `data/seeds/` doesn't matter. The 12 schemas below are the contract.

### `lobs` — `data/seeds/business/lobs.csv`

| column              | dtype  | notes |
|---------------------|--------|-------|
| `lob_id`            | Int64  | primary key. |
| `modelled_lob`      | String | natural key — what shows up in `ExposureAttribute`. |
| `rollup_lob`        | String | the rollup-level LOB. |
| `lob_type`          | String | classification. |
| `cds_cat_class_name`| String | drives currency derivation. |
| `office`            | String | drives forecast-factor join. |
| `class`             | String | drives forecast-factor join. |

### `perils` — `data/seeds/business/perils.csv`

| column         | dtype  | notes |
|----------------|--------|-------|
| `peril_id`     | Int64  | canonical PK; what every YLT row carries as `region_peril_id`. |
| `name`         | String | display label. |
| `region`       | String | `EU`, `UK`, … |
| `peril_family` | String | `WS`, `FL`, `EQ`, … **case-sensitive** — drives the flood-base-model rule. |

### `analyses` — `data/seeds/business/analyses.csv`

| column           | dtype  | notes |
|------------------|--------|-------|
| `vendor`         | String | `'verisk'` \| `'risklink'`. |
| `analysis_id`    | String | Verisk label or stringified RL analysis id. |
| `modelled_label` | String | display label e.g. `EU FL HD`. |
| `peril_id`       | Int64  | FK → `perils.peril_id`. |
| `lob_id`         | Int64  | nullable for verisk; populated for risklink. |

### `rollup_scope` — `data/seeds/business/rollup_scope.csv`

| column         | dtype   | notes |
|----------------|---------|-------|
| `modelled_lob` | String  | natural key from `lobs`. |
| `vendor`       | String  | `'verisk'` \| `'risklink'`. |
| `analysis_id`  | String  | the **modelled label** (e.g. `EU FL HD`), NOT the integer id. |
| `in_rollup`    | Boolean | `true` to include in the official rollup. |

### `blending_weights` — `data/seeds/vor/blending_weights.csv`

Long format. Generate with `uv run rollup derive-blending`
once `ep-summary-to-csv` has run.

| column        | dtype   | notes |
|---------------|---------|-------|
| `peril_id`    | Int64   | FK → `perils.peril_id`. |
| `peril_name`  | String  | denormalised display only. |
| `description` | String  | free-text reason. |
| `sub_peril`   | String  | nullable (sub-region splits). |
| `vendor`      | String  | `'verisk'` \| `'risklink'`. |
| `weight`      | Float64 | proportion; rl_weight + vk_weight should = 1.0 per peril. |

### `forecast_factors` — `data/seeds/vor/forecast_factors.csv`

Long format. Adding a forecast date is a data-only change.

| column          | dtype   | notes |
|-----------------|---------|-------|
| `class`         | String  | joins to `lobs.class`. |
| `office`        | String  | joins to `lobs.office`. |
| `office_iso2`   | String  | passthrough. |
| `forecast_date` | Date    | one of the per-tag forecast dates (e.g. `2026-01-01`). |
| `factor`        | Float64 | the multiplier. |

### `fx_rates` — `data/seeds/vor/fx_rates.csv`

| column            | dtype   | notes |
|-------------------|---------|-------|
| `currency_code`   | String  | source currency. |
| `target_currency` | String  | always `GBP` today. |
| `rate_date`       | Date    | snapshot date. |
| `rate`            | Float64 | source → target. |

### `euws_rate_factors` — `data/seeds/vor/euws_rate_factors.csv`

| column           | dtype   | notes |
|------------------|---------|-------|
| `model_event_id` | Int64   | joins to YLT `event_id` (verisk only). |
| `occ_year`       | Int64   | year of occurrence. |
| `factor`         | Float64 | per-event EUWS factor. |

### `euws_rank_overrides` — `data/seeds/adjustments/euws_rank_overrides.csv`

| column       | dtype   | notes |
|--------------|---------|-------|
| `rollup_lob` | String  | joins to `lobs.rollup_lob`. |
| `max_rank`   | Int64   | apply override when `rank ≤ max_rank`. |
| `factor`     | Float64 | replacement factor. |

### `fineart_adjustments` — `data/seeds/adjustments/fineart_adjustments.csv`

Optional. Empty = no fine-art adjustment (factor 1.0 for all rows).

| column                | dtype   | notes |
|-----------------------|---------|-------|
| `lob_id`              | Int64   | FK → `lobs.lob_id`. |
| `region_peril_id`     | Int64   | FK → `perils.peril_id`. |
| `applies_to_fa`       | Int64   | flag. |
| `rollup_region_peril` | String  | display. |
| `aal_factor`          | Float64 | applied today. |
| `tail_factor`         | Float64 | carried but not applied (future tail-loss work). |

### `air_events` — `data/seeds/validation/air_events.csv`

Verisk event catalogue. Optional stub.

| column     | dtype  | notes |
|------------|--------|-------|
| `event_id` | Int64  | matches YLT `EventID`. |
| `model_id` | Int64  | model code. |
| `event`    | Int64  | event number. |
| `year`     | Int64  | calendar year. |
| `day`      | Int64  | day of year. |

### `risklink_events` — `data/seeds/validation/risklink_events.csv`

RiskLink event catalogue. Optional stub.

| column     | dtype  | notes |
|------------|--------|-------|
| `event_id` | Int64  | matches YLT `eventid`. |
| `year`     | Int64  | calendar year. |
| `day`      | Int64  | day of year. |

---

## EP summaries — `data/ep_summaries/{vendor}/`

Vendor-supplied xlsx (multi-row header, wide RP columns) — **not** a
direct pipeline input. Convert to long format with:

    uv run rollup ep-summary-to-csv

The resulting `<stem>.long.csv` has `(id, rp, ep_type, lob, region_peril, gl)`
for risklink (`STG_RISKLINK_EP` schema).

Then derive blending weights:

    uv run rollup derive-blending

This rewrites `data/seeds/vor/blending_weights.csv` from the AAL totals.

---

## Outputs — `data/output/`

The pipeline writes here. **Created on run.**

- `HiscoAIR_<yyyymm>_main.parquet` (one per forecast tag)
- `HiscoAIR_dialsup.parquet` (one per vendor; raw loss / FX only)
- `HiscoRMS_<yyyymm>_main.parquet`
- `HiscoRMS_dialsup.parquet`
- `mts_tbl_ylt_combined_all_factors.parquet` (long format; full audit trail with all factor scalars and blending proportions)
- `debug/audit_wide.parquet`, `debug/audit_long.parquet` — only when `--dump-interim` is passed.

---

## Validating before a run

```bash
uv run rollup --dry-run
```

Per file the plan reports:

- ✓ when schema matches.
- ✘ with a column-level diff: `missing=[col1], wrong_dtype=[col2:Float64→Int64], unexpected=[col3]`.

If the plan is green you can run; if not, the failure tells you exactly
which file, which column, and what's wrong.
