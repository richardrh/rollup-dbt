# Data requirements

Schema reference. For the step-by-step procedure, see [Loading your data](load-data.md).

**The contract:** if every file below exists in the right shape, `uv run rollup --yes` runs end-to-end and writes 9 parquets to `data/output/`. The preflight (`uv run rollup --dry-run`) reports the status of each.

## Layout the pipeline expects

```
<repo>/
├── polars/                   ← SOURCE CODE — nothing for you to touch here
└── data/                     ← ALL user-owned input/output
    ├── seeds/                ← reference CSVs (git-tracked; 4 stubs need populating)
    ├── ylt/
    │   ├── verisk/*.parquet
    │   └── risklink/*.parquet
    ├── ep_summaries/         ← optional unless --derive-blending is selected
    │   ├── verisk/*.csv
    │   └── risklink/*.csv
    └── output/               ← pipeline writes here
```

See [`../polars/RH-TODO-DATA.md`](../polars/RH-TODO-DATA.md) for the
simple collect-these-files checklist — this doc is the detailed schema
reference the checklist points at.

Override paths persistently in `rollup.local.toml` (`[paths]` and
`[vendors.<vendor>]`) or per process with `ROLLUP_DATA_DIR`,
`ROLLUP_SEEDS_DIR`, `ROLLUP_OUTPUT_DIR`, `ROLLUP_YLT_VERISK_DIR`,
`ROLLUP_YLT_RISKLINK_DIR`, `ROLLUP_EP_VERISK_DIR`, `ROLLUP_EP_RISKLINK_DIR`.

---

## A0. EP-summary xlsx — converting to long-format CSV

The vendor-supplied EP-summary xlsx files use a multi-row header and wide
RP columns. To convert them into long-format CSVs that match the
`STG_RISKLINK_EP` / `STG_VERISK_EP` schemas, run:

    uv run rollup ep-summary-to-csv

For each xlsx found under `data/ep_summaries/{vendor}/`, a sibling
`<stem>.long.csv` is written. The long format is `(id, rp, ep_type, lob,
region_peril, gl)` for risklink.

AAL rows have `rp = 0`; OEP and AEP rows have the return period as `rp`
(e.g. `2`, `5`, ..., `10000`).

### Deriving blending weights from EP summaries

Normal runs use the reviewed `data/seeds/vor/blending_weights.csv` seed. After
converting xlsx to long CSV (above), use `uv run rollup --derive-blending` to
derive blending weights in memory for one run and write an audit copy to
`data/output/debug/derived_blending_weights.csv`; this does **not** overwrite
the reviewed seed. To deliberately refresh the reviewed seed, run:

    uv run rollup derive-blending

Reads the `*.long.csv` files under `data/ep_summaries/{vendor}/`,
computes per-peril totals for AAL, 1-in-200 OEP, 1-in-1000 OEP, and
1-in-10000 OEP, and writes
`data/seeds/vor/blending_weights.csv` with proportions:

    rl_proportion = rl_aal / (rl_aal + vk_aal)
    vk_proportion = 1 - rl_proportion

`uv run rollup --use-blending-seed` and `uv run rollup --no-derive-blending`
are explicit aliases for the default seed-backed behavior.

---

## A. YLT parquets — the actual loss tables

Two directories, one per vendor. Each may contain multiple chunks
(`air_ylt_c1.parquet`, `air_ylt_c2.parquet`, …) — they are scanned as a
single lazy table.

### `data/ylt/verisk/air_ylt_*.parquet` (≈ 10 000 simulation years)

Wire schema (matches AIR Touchstone export — CamelCase preserved):

| column              | type     | notes |
| ------------------- | -------- | ----- |
| `Analysis`          | String   | label e.g. `EU_WS`; joined to `analyses.modelled_label` after numeric `analysis_id` filtering. |
| `ExposureAttribute` | String   | the LOB on this row, e.g. `HIC_HH_UK`; joined to `lobs.modelled_lob`. |
| `CatalogTypeCode`   | String   | filtered to `'STC'` (matches duckdb `int_vw_vk_ylt`). |
| `EventID`           | Int64    | event identifier, used for the EUWS join. |
| `ModelCode`         | Int64    | passed through to Hisco. |
| `YearID`            | Int64    | simulation year, 1..n_simulations. |
| `PerilSetCode`      | Int64    | not used in the rollup, validated for shape. |
| `GroundUpLoss`      | Float64  | not used; `NetOfPreCatLoss` is the loss column. |
| `GrossLoss`         | Float64  | not used. |
| `NetOfPreCatLoss`   | Float64  | the loss carried into the chain. |
| `filename`          | String   | passthrough. |

### `data/ylt/risklink/risklink_ylt_*.parquet` (≈ 100 000 simulation years)

Wire schema (matches RiskLink export — lowercase):

| column            | type     | notes |
| ----------------- | -------- | ----- |
| `SimulationSetId` | Int64    | passthrough. |
| `yearid`          | Int64    | simulation year. |
| `eventid`         | Int64    | event identifier. |
| `date`            | String   | passthrough. |
| `p_value`         | Float64  | passthrough. |
| `anlsid`          | Int64    | analysis id; cast to String and joined to `analyses.analysis_id` (vendor='risklink' rows). |
| `name`            | String   | passthrough. |
| `description`     | String   | passthrough. |
| `rate`            | Float64  | passthrough. |
| `meanloss`        | Float64  | passthrough. |
| `stddev`          | Float64  | passthrough. |
| `expvalue`        | Float64  | passthrough. |
| `loss`            | Float64  | the loss carried into the chain. |

If `n_simulations` differs from 10 000 / 100 000, override
`Vendor.n_simulations` in `rollup/config.py` — it drives the AAL division in
`attach_uplift` and the `rp = n_sim / rank` bucket selection in `attach_rank`.

### Which RiskLink analyses do you actually need to export?

You don't need a per-event YLT for every RiskLink analysis under the sun.
Two pieces of pipeline logic determine the scope:

#### 1. The `base_model` rule (which event_ids end up in the output)

`attach_uplift` in `polars/rollup/stages/factors.py` reads `base_model` from
`blending_weights.csv`. The generated seed defaults flood perils to RiskLink
and all other perils to Verisk, but the seed is the runtime lookup:

```
base_model = blending_weights.base_model  -- per peril_id lookup
```

**The `base_model` choice decides which vendor's `event_id` appears in the
final Hisco fanout.** For flood, the RMS Hisco files report RiskLink
`event_id`s and dates. For wind / EQ / everything else, they report Verisk
`event_id`s. So:

> **Per-event RiskLink YLTs are only strictly needed for `peril_family = 'FL'`** —
> i.e. every analysis in `analyses.csv` whose `peril_id` resolves to a
> flood peril (Europe Flood = 2, UK Flood = 4, plus EU sub-perils like
> `BE FL`, `DE FL`).

#### 2. The blending weights (which AALs go into the blend)

`attach_uplift` also computes:

```
blended_AAL = vk_proportion × vk_AAL + rl_proportion × rl_AAL
uplift_factor = blended_AAL / base_model_AAL
```

So even for non-flood perils, **if `blending_weights.weight > 0` for
`vendor='risklink'`**, the pipeline still needs an `rl_AAL` for that peril
— and computing that today requires per-event RiskLink YLT data.

If `blending_weights` for a non-flood peril sets `risklink.weight = 0`,
`rl_AAL` contributes nothing and the RiskLink YLT for that peril is
optional.

#### Practical guidance for the data-export request

Send the exporter **two lists**, both derived from the seeds:

1. **Required (per-event YLT):** every RiskLink analysis_id whose `peril_id`
   has `base_model = 'risklink'` in `blending_weights.csv`. These drive the
   RMS event-by-event output.
2. **Optional (per-event YLT or summary AAL):** every analysis_id whose
   peril has `blending_weights.weight > 0` for risklink AND
   `peril_family != 'FL'`. The pipeline today needs per-event data; if
   you set `risklink.weight = 0` for those perils in `blending_weights`,
   you can skip them entirely.

Use this query to enumerate the lists:

```sql
-- Required (base_model='risklink')
SELECT a.analysis_id, a.modelled_label, p.peril_id, p.name AS peril_name
FROM analyses a
JOIN perils  p ON a.peril_id = p.peril_id
JOIN (SELECT DISTINCT peril_id, base_model FROM blending_weights) bm
  ON bm.peril_id = p.peril_id
WHERE a.vendor = 'risklink' AND bm.base_model = 'risklink'
ORDER BY p.peril_id, a.analysis_id;

-- Optional (non-flood, only if blending_weights.risklink > 0)
SELECT DISTINCT a.analysis_id, a.modelled_label, p.peril_id, p.name, bw.weight AS rl_weight
FROM analyses a
JOIN perils  p  ON a.peril_id = p.peril_id
JOIN blending_weights bw
  ON bw.peril_id = p.peril_id AND bw.vendor = 'risklink'
WHERE a.vendor = 'risklink' AND bw.base_model != 'risklink' AND bw.weight > 0
ORDER BY p.peril_id, a.analysis_id;
```

#### What the parquet must look like

- **One row per `(yearid, eventid, anlsid)`** — *not* a per-period summary.
  We received a year-aggregated summary export earlier; the pipeline cannot
  use it because OEP requires the largest event in a year, not the year total.
- **Filter to `PERSPCODE = 'RL'`** (ground-up loss) before exporting; the
  pipeline assumes a single perspective.
- **Parquet preferred**, CSV acceptable.
- All 13 columns in the wire schema must be present; passthrough columns
  (`p_value`, `meanloss`, `stddev`, `expvalue`, `rate`, `name`, `description`)
  may be null/zero if the export tool can't supply them.

---

## B. Seeds — `data/seeds/*.csv` (11 files)

11 CSVs total. The business/VOR seeds are required for real runs; event
catalogues may be stub-empty but improve validation/enrichment when populated.

### Already populated (in git)

| seed                       | rows  | source                                                                   | refresh cadence |
| -------------------------- | ----- | ------------------------------------------------------------------------ | --------------- |
| `lobs.csv`                 | 62    | `dbt/seeds/hisco-org/hisco_org__lobs.csv`                                | when LOB list changes |
| `euws_rate_factors.csv`    | 69 212| `dbt/seeds/vor/vor_euws_rate_factors.csv`                                | when vor model changes |
| `euws_rank_overrides.csv`  | 1     | hand-curated; one row per per-LOB rank-threshold override                | rare |
| `forecast_factors.csv`     | 78    | `dbt/seeds/hisco-org/hisco_org__forecast_factors.csv`                    | every forecast cycle |
| `fx_rates.csv`             | 6     | **handcrafted** — replace with a real FX snapshot before any prod run    | every snapshot |

### Stubs to populate (the four-way split)

All four derive from the same logical sources: the peril, analysis, and LOB
dimensions. Typically you'll export these as a single duckdb session or
equivalent data-transformation pipeline.

#### 1. `perils.csv` — peril dimension (REQUIRED)

One row per peril. `peril_family` ("FL", "WS", "EQ", …) drives the flood-base-model rule.

| column         | type    | notes |
| -------------- | ------- | ----- |
| `peril_id`     | Int64   | primary key. |
| `name`         | String  | display label. |
| `region`       | String  | `EU`, `UK`, `US`, … |
| `peril_family` | String  | `WS`, `FL`, `EQ`, `TC`, `CS`, `WF`. **Must be `FL` (uppercase) for flood perils — case sensitive.** |

**Must have exactly one row per `peril_id`.** Collapse duplicates with `GROUP BY peril_id` if needed. Normalise `peril_family` to uppercase.

#### 2. `analyses.csv` — vendor analysis → peril (+ lob for RiskLink) (REQUIRED)

Maps each vendor's analysis to peril. For Verisk, `lob_id` is NULL (lob comes from YLT). For RiskLink, `lob_id` is populated.

| column           | type    | notes |
| ---------------- | ------- | ----- |
| `vendor`         | String  | `'verisk'` or `'risklink'`. |
| `analysis_id`    | String  | numeric vendor analysis id, stored as text. Bundled Verisk IDs are placeholders. |
| `modelled_label` | String  | vendor label used in Verisk `Analysis`, operator review, and EP labels. |
| `peril_id`       | Int64   | FK into `perils.csv`. |
| `lob_id`         | Int64   | FK into `lobs.csv`; NULL for Verisk, populated for RiskLink. |

One row per vendor × analysis pair. Concatenate Verisk and RiskLink CSVs (header from one, body of both).

#### 3. `valid_analyses.csv` — numeric vendor analysis allow-list (REQUIRED)

Pipeline filters YLT and EP summaries to listed `(vendor, analysis_id)` rows.
Empty file → zero rows output.

| column        | type   | notes |
| ------------- | ------ | ----- |
| `vendor`      | String | `'verisk'` or `'risklink'`. |
| `analysis_id` | String | numeric vendor analysis id, stored as text for both vendors. Replace bundled Verisk placeholders with real IDs before production. |

#### 4. `blending_weights.csv` — long-format blend weights (REQUIRED)

One row per (peril, vendor) pair. `sub_peril` is optional regional split label.

| column        | type    | notes |
| ------------- | ------- | ----- |
| `peril_id`    | Int64   | FK into `perils.csv`. |
| `peril_name`  | String  | display-only; not used in joins. |
| `description` | String  | free-text (e.g. `"default 50/50"`). Empty string fine. |
| `sub_peril`   | String  | regional split label (e.g. `216a`); NULL for unconditional weight. |
| `vendor`      | String  | `'verisk'` or `'risklink'`. |
| `weight`      | Float64 | blend weight, 0..1. |

### Optional seeds (improve output quality)

**`verisk_events.parquet`** — Verisk event catalogue. Used to enrich Verisk `ModelEventID`/`ModelEventDay`; missing rows report orphan warnings.

**`risklink_flood22_model_events.parquet`** — RiskLink event catalogue. Used to enrich `ModelEventDay` for the January-style `rl_withdayid` fanout.

**`fx_rates.csv`** — FX snapshot (GBP target). Handcrafted; refresh before production runs.

---

## C. Currency derivation — pattern in `cds_cat_class_name`

`attach_currency` derives the row's `required_currency` from the
`cds_cat_class_name` column on `lobs.csv`:

| substring (space-padded) | currency |
| ------------------------ | -------- |
| ` UK ` (e.g. `HIC UK Household`)  | GBP |
| ` EU ` (e.g. `HSA EU Fine Art`)   | EUR |
| anything else                     | GBP (fallback) |

If you want a different mapping, update `attach_currency` and add the new
member to `CurrencyCode` in `rollup/config.py`. **Every currency code that
can fall out of this rule must have a row in `fx_rates.csv` with
`target_currency = GBP`**, otherwise the pipeline aborts with
`MissingFxRateError` rather than silently using rate 1.0.

---

## D. Forecast-factor join

`forecast_factors.csv` is keyed on `(office, class, forecast_date)`. After
staging, every row in the YLT has `office` and `lob_class` columns
(forwarded from `lobs.csv`). The forecast-factor seed must use the **exact
same office strings** as `lobs.csv`. A mismatch silently degrades to
factor=1.0 for that LOB (intentional, documented), so check the audit_wide
dump for `f_{tag}` columns that are all 1.0 — that's the diagnostic.

---

## E. Default long-format output — `mts_tbl_ylt_combined_all_factors.parquet`

Written unconditionally on every run to `data/output/`. One row per
(YLT event, metric). Columns:

- All identity dims (vendor, lob, peril, region, year_id, event_id, ...)
- The blending factors: `rl_proportion`, `vk_proportion`, `base_model`
- `metric_name` (string) — one of the chain stages or `dialsup`
- `value` (float) — the metric value

This matches january's `mts_tbl_ylt_combined_all_factors` table for diff-friendliness.
The wide audit dump is written to `data/output/debug/` by default; use
`--no-audit` to skip it.

---

## F. Adding a new forecast date

Cheapest change in the codebase. To add `2027-07-01`:

1. Edit `data/seeds/forecast_factors.csv` and add one row per
   `(class, office)` combination with `forecast_date=2027-07-01`.
2. Run the pipeline. New `f_202707` column + three new metric columns +
   two new Hisco parquets per vendor.

No code change. No test change.

---

## G. Verifying the pipeline works on your data

```bash
uv run rollup --dry-run        # plan: every seed + YLT + EP file checked
uv run rollup                  # interactive wizard
uv run rollup --yes            # full run
uv run rollup --yes --no-audit             # skip audit_{wide,long}.parquet
```

If everything is green:

- `data/output/Hisco{AIR,RMS}_{yyyymm}_main.parquet` — one per vendor and forecast date.
- `data/output/Hisco{AIR,RMS}_dialsup.parquet` — one sensitivity output per vendor.
- `data/output/debug/audit_wide.parquet` — every event with the factor chain
  laid out left-to-right (default; skipped only with `--no-audit`).

To verify the code itself works end-to-end before pointing at production:

```bash
uv run python -m pytest polars/tests/test_e2e.py -v
```

The e2e suite builds a synthetic dataset under `polars/tests/data/`,
runs the pipeline against it (using exactly the same code paths as a
production run), and asserts:

1. The correct count of Hisco parquets is written.
2. Every parquet matches the `HISCO_FANOUT` schema.
3. At least one variant has non-zero `ModelGrossLoss`.
4. The audit-dump column ordering is the documented left-to-right chain.

## If something breaks

See [Troubleshooting](troubleshooting.md) for common failure modes and fixes.
