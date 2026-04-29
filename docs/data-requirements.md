# Data requirements — what you need to provide for a real run

This is the contract between the pipeline and the data you supply. If every
file listed below exists in the right shape, `python -m rollup.pipeline --yes`
runs end-to-end and writes 12 (= 2 vendors × N forecast dates × 2 flavours)
`Hisco{AIR,RMS}_{date}_{flavour}.parquet` files under `data/output/`.

The pipeline's preflight (`python -m rollup.pipeline --dry-run`) reports the
status of every file mentioned here. Read its output before re-checking this
doc.

## Layout the pipeline expects

```
<repo>/
├── polars/                   ← SOURCE CODE — nothing for you to touch here
└── data/                     ← ALL user-owned input/output
    ├── seeds/                ← reference CSVs (git-tracked; 4 stubs need populating)
    ├── ylt/
    │   ├── verisk/*.parquet
    │   └── risklink/*.parquet
    ├── ep_summaries/         ← optional; only used by tests/test_integration_ep.py
    │   ├── verisk/*.csv
    │   └── risklink/*.csv
    └── output/               ← pipeline writes here
```

See [`../polars/RH-TODO-DATA.md`](../polars/RH-TODO-DATA.md) for the
simple collect-these-files checklist — this doc is the detailed schema
reference the checklist points at.

Override any path with `ROLLUP_DATA_DIR`, `ROLLUP_SEEDS_DIR`,
`ROLLUP_OUTPUT_DIR`, `ROLLUP_YLT_VERISK_DIR`, `ROLLUP_YLT_RISKLINK_DIR`,
`ROLLUP_EP_VERISK_DIR`, `ROLLUP_EP_RISKLINK_DIR`.

---

## A0. EP-summary xlsx — converting to long-format CSV

The vendor-supplied EP-summary xlsx files use a multi-row header and wide
RP columns. To convert them into long-format CSVs that match the
`STG_RISKLINK_EP` / `STG_VERISK_EP` schemas, run:

    uv run -m rollup.pipeline ep-summary-to-csv

For each xlsx found under `data/ep_summaries/{vendor}/`, a sibling
`<stem>.long.csv` is written. The long format is `(id, rp, ep_type, lob,
region_peril, gl)` for risklink.

AAL rows have `rp = 0`; OEP and AEP rows have the return period as `rp`
(e.g. `2`, `5`, ..., `10000`).

---

## A. YLT parquets — the actual loss tables

Two directories, one per vendor. Each may contain multiple chunks
(`air_ylt_c1.parquet`, `air_ylt_c2.parquet`, …) — they are scanned as a
single lazy table.

### `data/ylt/verisk/air_ylt_*.parquet` (≈ 10 000 simulation years)

Wire schema (matches AIR Touchstone export — CamelCase preserved):

| column              | type     | notes |
| ------------------- | -------- | ----- |
| `Analysis`          | String   | label e.g. `EU_WS`; joined to `analyses.analysis_id` (vendor='verisk' rows). |
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
`attach_uplift`.

### Which RiskLink analyses do you actually need to export?

You don't need a per-event YLT for every RiskLink analysis under the sun.
Two pieces of pipeline logic determine the scope:

#### 1. The `base_model` rule (which event_ids end up in the output)

`attach_uplift` in `polars/rollup/stages/factors.py` assigns one `base_model`
per `(lob_id, region_peril_id)` group:

```
base_model = 'risklink'  if peril_family == 'FL'   (i.e. any flood peril)
base_model = 'verisk'    otherwise                 (wind, EQ, etc.)
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

1. **Required (per-event YLT):** every analysis_id where `peril_id`
   resolves to `peril_family = 'FL'`. These drive the RMS event-by-event
   output — every flood event in the rollup must be present.
2. **Optional (per-event YLT or summary AAL):** every analysis_id whose
   peril has `blending_weights.weight > 0` for risklink AND
   `peril_family != 'FL'`. The pipeline today needs per-event data; if
   you set `risklink.weight = 0` for those perils in `blending_weights`,
   you can skip them entirely.

Use this query to enumerate the lists:

```sql
-- Required (flood perils — base_model='risklink')
SELECT a.analysis_id, a.modelled_label, p.peril_id, p.name AS peril_name
FROM analyses a
JOIN perils  p ON a.peril_id = p.peril_id
WHERE a.vendor = 'risklink' AND p.peril_family = 'FL'
ORDER BY p.peril_id, a.analysis_id;

-- Optional (non-flood, only if blending_weights.risklink > 0)
SELECT a.analysis_id, a.modelled_label, p.peril_id, p.name, bw.weight AS rl_weight
FROM analyses a
JOIN perils  p  ON a.peril_id = p.peril_id
JOIN blending_weights bw
  ON bw.peril_id = p.peril_id AND bw.vendor = 'risklink'
WHERE a.vendor = 'risklink' AND p.peril_family != 'FL' AND bw.weight > 0
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

## B. Seeds — `data/seeds/*.csv` (11 files, all required)

The pre-run plan reporter flags any seed that is missing or whose header
doesn't match the declared schema. The pipeline will not run if
`all_seeds_ok` is false.

The peril dimension has been **split into four single-purpose tables**.
january's god-table `dim_region_perils` (which mixed peril labels, vendor
mapping, blending FKs, and per-LOB applies-to flags into one row) is gone.
The new tables — perils, analyses, rollup_scope, blending_weights —
each have one job. The data the user must supply is exactly the same
columns that lived on `dim_region_perils`, just split up coherently.

### Already populated (in git)

| seed                       | rows  | source                                                                   | refresh cadence |
| -------------------------- | ----- | ------------------------------------------------------------------------ | --------------- |
| `lobs.csv`                 | 62    | `dbt/seeds/hisco-org/hisco_org__lobs.csv`                                | when LOB list changes |
| `euws_rate_factors.csv`    | 69 212| `dbt/seeds/vor/vor_euws_rate_factors.csv`                                | when vor model changes |
| `euws_rank_overrides.csv`  | 1     | hand-curated; one row per per-LOB rank-threshold override                | rare |
| `forecast_factors.csv`     | 78    | `dbt/seeds/hisco-org/hisco_org__forecast_factors.csv`                    | every forecast cycle |
| `fx_rates.csv`             | 6     | **handcrafted** — replace with a real FX snapshot before any prod run    | every snapshot |

### Stubs to populate from `loader.main.dim_region_perils` (the four-way split)

All four come from the same source — `dim_region_perils` plus
`dim_rl_analysis` plus `lobs`. One duckdb session, four `COPY` statements.

#### 1. `perils.csv` — peril dimension (REQUIRED)

One row per canonical peril. The integer `peril_id` is what every YLT row
carries as `region_peril_id` after staging. `peril_family` ("FL", "WS",
"EQ", …) drives the flood-base-model rule — `attach_uplift` forces
`base_model='risklink'` for any row whose `peril_family == "FL"`.

| column         | type    | notes |
| -------------- | ------- | ----- |
| `peril_id`     | Int64   | canonical primary key (matches january's `dim_region_perils.id` integer values). |
| `name`         | String  | display label, e.g. `"Europe Winter Storm"`. |
| `region`       | String  | `EU`, `UK`, `US`, … |
| `peril_family` | String  | `WS`, `FL`, `EQ`, `TC`, `CS`, `WF`. **Used for the flood check** — case sensitive. |

**The output must have one row per `peril_id`** — the staging join
(`_peril_dim` in `staging.py`) is keyed on `peril_id` and will multiply
YLT rows if there are duplicates. `dim_region_perils` has multiple rows
per `blending_factor_region_peril_id` (one per vendor, plus per
modelled-label-variant like `UK_WSSS` vs `UK_WSSS_GCAdj`). The query
below collapses those with a `GROUP BY` so you get exactly one row per
peril. `peril_family` is also normalised to upper-case + trimmed to
guarantee the flood check (`peril_family == "FL"`) matches.

```sql
COPY (
  SELECT
    blending_factor_region_peril_id              AS peril_id,
    MIN(rollup_region_peril)                     AS name,
    MIN(region)                                  AS region,
    UPPER(TRIM(MIN(peril)))                      AS peril_family
  FROM loader.main.dim_region_perils
  GROUP BY blending_factor_region_peril_id
  ORDER BY peril_id
) TO 'data/seeds/perils.csv' WITH (HEADER, DELIMITER ',');
```

**After exporting, sanity-check uniqueness**:
```sql
-- Should return zero rows.
SELECT peril_id, COUNT(*) FROM read_csv('data/seeds/perils.csv')
GROUP BY peril_id HAVING COUNT(*) > 1;
```

If `MIN(rollup_region_peril)` picks an unwanted variant for any
peril (e.g. `UK_WSSS` instead of `UK_WSSS_GCAdj`), edit the CSV
manually after export — `name` is display-only, the pipeline doesn't
join on it.

#### 2. `analyses.csv` — vendor analysis → peril (+ lob for RiskLink) (REQUIRED)

Without this, neither vendor's YLT can resolve `(modelled_label) → peril_id`.
For Verisk the analysis is peril-only (`lob_id` NULL — lob comes from the
YLT row's `ExposureAttribute`). For RiskLink the analysis IS the (lob,
peril) pair, so `lob_id` is populated.

| column           | type    | notes |
| ---------------- | ------- | ----- |
| `vendor`         | String  | `'verisk'` or `'risklink'`. |
| `analysis_id`    | String  | the wire label — Verisk text label or stringified `rl_analysis_id`. |
| `modelled_label` | String  | display label (often same as `analysis_id`). This is what the YLT carries as `MODELLED_REGION_PERIL` after staging — and what `rollup_scope.analysis_id` references. |
| `peril_id`       | Int64   | FK into `perils.csv`. |
| `lob_id`         | Int64   | FK into `lobs.csv`; NULL for Verisk. |

```sql
-- Verisk rows: one per modelled_region_peril where vendor='verisk'
COPY (
  SELECT
    'verisk'                                      AS vendor,
    modelled_region_peril                         AS analysis_id,
    modelled_region_peril                         AS modelled_label,
    blending_factor_region_peril_id               AS peril_id,
    NULL::BIGINT                                  AS lob_id
  FROM loader.main.dim_region_perils
  WHERE vendor = 'verisk'
  ORDER BY modelled_region_peril
) TO 'data/seeds/analyses_verisk.csv' WITH (HEADER, DELIMITER ',');

-- RiskLink rows: one per (rl_analysis_id, lob)
COPY (
  SELECT
    'risklink'                                    AS vendor,
    CAST(dra.rl_analysis_id AS VARCHAR)           AS analysis_id,
    dra.region_peril                              AS modelled_label,
    rp.blending_factor_region_peril_id            AS peril_id,
    lobs.id                                       AS lob_id
  FROM loader.main.dim_rl_analysis AS dra
  INNER JOIN loader.main.dim_region_perils AS rp
    ON rp.modelled_region_peril = dra.region_peril AND rp.vendor = 'risklink'
  INNER JOIN reference.lobs AS lobs
    ON lobs.modelled_lob = dra.lob
  ORDER BY dra.rl_analysis_id
) TO 'data/seeds/analyses_risklink.csv' WITH (HEADER, DELIMITER ',');

-- Concatenate (header from one, body of both)
-- $ cat analyses_verisk.csv > data/seeds/analyses.csv
-- $ tail -n +2 analyses_risklink.csv >> data/seeds/analyses.csv
```

**NULL handling**: DuckDB COPY writes `NULL::BIGINT` as an empty CSV
cell. Polars parses empty cells as `null` for `Int64` columns when an
explicit schema is provided — which `seeds.py` always does. So the
verisk rows look like `verisk,EU_WS,EU_WS,206,` (trailing comma) in the
CSV — that's correct.

**Sanity-check after export**:
```sql
-- Verisk lob_id should always be NULL, RiskLink lob_id should never be NULL.
SELECT vendor, COUNT(*) FILTER (WHERE lob_id IS NULL)  AS null_lob,
                COUNT(*) FILTER (WHERE lob_id IS NOT NULL) AS populated_lob
FROM read_csv('data/seeds/analyses.csv')
GROUP BY vendor;
-- Expect: verisk → null_lob = N, populated_lob = 0
--         risklink → null_lob = 0, populated_lob = N
```

#### 3. `rollup_scope.csv` — which (lob, vendor, analysis) triples are official (REQUIRED)

Replaces january's `applies_to_{mga,prop,fa}` flag fan-out. The pipeline
inner-joins the YLT to this seed and drops anything not marked `True`. If
this file is empty the pipeline returns zero rows.

| column        | type    | notes |
| ------------- | ------- | ----- |
| `lob_id`      | Int64   | FK into `lobs.csv`. |
| `vendor`      | String  | `'verisk'` or `'risklink'`. |
| `analysis_id` | String  | the **modelled label** (matches `analyses.modelled_label`, NOT the raw RL integer). |
| `in_rollup`   | Boolean | True to keep, False to drop. |

```sql
COPY (
  SELECT
    lobs.id                              AS lob_id,
    rp.vendor,
    rp.modelled_region_peril             AS analysis_id,
    CASE lobs.lob_type
      WHEN 'mga'  THEN rp.applies_to_mga
      WHEN 'prop' THEN rp.applies_to_prop
      WHEN 'fa'   THEN rp.applies_to_fa
      ELSE 0
    END :: BOOLEAN                       AS in_rollup
  FROM reference.lobs AS lobs
  CROSS JOIN loader.main.dim_region_perils AS rp
  ORDER BY lobs.id, rp.vendor, rp.modelled_region_peril
) TO 'data/seeds/rollup_scope.csv' WITH (HEADER, DELIMITER ',');
```

#### 4. `blending_weights.csv` — long-format blend weights (REQUIRED)

Replaces the wide `air_blend` / `rms_blend` / `kat_risk_blend` columns of
`blending_factors`. One row per (peril_id, sub_peril, vendor). `sub_peril`
is nullable — most perils don't need regional sub-splits.

| column        | type    | notes |
| ------------- | ------- | ----- |
| `peril_id`    | Int64   | FK into `perils.csv`. |
| `peril_name`  | String  | denormalised from `perils.name` for human readability — pipeline NEVER joins on this; populate from the same source row as `peril_id`. |
| `description` | String  | free-text reason this row exists (e.g. `"default 50/50"` or `"Germany sub-peril split"`). Empty string is fine. |
| `sub_peril`   | String  | regional split label (`216a`, `216b`, …); NULL for the unconditional weight. |
| `vendor`      | String  | `'verisk'` or `'risklink'`. (other vendors are silently ignored by `attach_uplift`.) |
| `weight`      | Float64 | the blend weight, 0..1. |

The `peril_name` and `description` columns are required by the schema
(strict validation at seed load) but are **never used in joins** — they
exist so the CSV is browsable. Pipe `''` (empty string) into
`description` if you have nothing meaningful to say.

```sql
-- Long-pivot the wide air_blend/rms_blend columns. Joins dim_region_perils
-- to bring in the human-readable `peril_name`. Adjust the description text
-- per row if you want — empty string is acceptable.
COPY (
  WITH bf AS (
    SELECT bf.region_peril_id      AS peril_id,
           drp.rollup_region_peril AS peril_name,
           bf.sub_region_peril_id  AS sub_peril,
           bf.air_blend, bf.rms_blend
    FROM loader.main.blending_factors AS bf
    -- pick ONE drp row per peril_id (any vendor, any modelled label) just for the display name
    LEFT JOIN (
      SELECT DISTINCT ON (blending_factor_region_peril_id)
             blending_factor_region_peril_id AS peril_id,
             rollup_region_peril
      FROM loader.main.dim_region_perils
      ORDER BY blending_factor_region_peril_id
    ) AS drp ON drp.peril_id = bf.region_peril_id
  )
  SELECT peril_id, peril_name,
         '' AS description,
         sub_peril, 'verisk' AS vendor, air_blend AS weight
  FROM bf
  UNION ALL
  SELECT peril_id, peril_name,
         '' AS description,
         sub_peril, 'risklink' AS vendor, rms_blend AS weight
  FROM bf
  ORDER BY peril_id, sub_peril, vendor
) TO 'data/seeds/blending_weights.csv' WITH (HEADER, DELIMITER ',');
```

If your duckdb version doesn't have `DISTINCT ON`, swap that subquery
for `(SELECT peril_id, MIN(rollup_region_peril) AS rollup_region_peril
FROM loader.main.dim_region_perils GROUP BY blending_factor_region_peril_id)`
or similar.

### Recommended (silences a warning)

#### 5. `air_events.csv` — Verisk event catalogue

Without this, the pipeline still runs — but `count_event_id_orphans` will
report 100 % orphans and the warning is noise. Populate to silence it.

| column     | type  | notes |
| ---------- | ----- | ----- |
| `event_id` | Int64 | matches YLT `EventID`. |
| `model_id` | Int64 | matches YLT `ModelCode`. |
| `event`    | Int64 | passthrough. |
| `year`     | Int64 | simulation year. |
| `day`      | Int64 | day-of-year. |

```sql
COPY (
  SELECT EventID AS event_id, ModelID AS model_id,
         "Event" AS event, "Year" AS year, "Day" AS day
  FROM reference.air_events
  ORDER BY event_id
) TO 'data/seeds/air_events.csv' WITH (HEADER, DELIMITER ',');
```

### Optional but improves output

#### 6. `fineart_adjustments.csv` — fine-art gross-to-net (OPTIONAL)

Stub-empty by default. Without this, `attach_fagross` returns
`fa_gross_aal_factor = 1.0` for every row (multiplicative pass-through), so
fine-art LOBs flow through unadjusted. Populate to apply the real
adjustment.

| column                 | type    |
| ---------------------- | ------- |
| `lob_id`               | Int64   |
| `region_peril_id`      | Int64   |
| `applies_to_fa`        | Int64   |
| `rollup_region_peril`  | String  |
| `aal_factor`           | Float64 |
| `tail_factor`          | Float64 (currently audit-only — see note in `AllFactorsCol.FA_GROSS_TAIL_FACTOR`) |

```sql
COPY (
  SELECT lob_id, region_peril_id, applies_to_fa, rollup_region_peril,
         aal_factor, tail_factor
  FROM reference.fineart_gross_to_net_adjustment2
  ORDER BY lob_id, region_peril_id
) TO 'data/seeds/fineart_adjustments.csv' WITH (HEADER, DELIMITER ',');
```

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
The wide audit dump is still gated behind `--dump-interim` and writes to `data/output/debug/`.

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
cd polars
uv run python -m rollup.pipeline --dry-run        # plan: every seed + YLT + EP file checked
uv run python -m rollup.pipeline --yes            # full run
uv run python -m rollup.pipeline --yes -d         # also write audit_{wide,long}.parquet
```

If everything is green:

- `data/output/Hisco{AIR,RMS}_{date}_{main,dialsup}.parquet` — one per variant.
- `data/output/debug/audit_wide.parquet` — every event with the factor chain
  laid out left-to-right (only when `-d` is set).

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

---

## Quick failure-mode reference

| symptom | cause | fix |
| ------- | ----- | --- |
| `seed 'X' missing at /…/X.csv` | a seed file isn't where the loader expected | put the file at `data/seeds/X.csv` (or set `ROLLUP_SEEDS_DIR`). |
| `[seed.X] missing columns: [...]` | seed CSV header drifted | rename headers in the CSV to match the schema in `rollup/schemas/columns.py`. |
| `[seed.X] dtype mismatches` | seed has the right column names but wrong types | re-export from duckdb with explicit casts, or fix the CSV. |
| `MissingFxRateError: fx_rates.csv has no GBP rate for currencies: ['EUR']` | a YLT row needs a currency you haven't supplied | add a row to `fx_rates.csv`. |
| `event-id orphans for vendor=verisk: N / M YLT rows have no match in air_events` | `air_events.csv` is empty or out of date | populate from `reference.air_events`. The pipeline continues — observation only. |
| Hisco parquets are written but every `ModelGrossLoss = 0` | a join earlier in the chain returned 0 rows | run `--dump-interim` and inspect `audit_wide.parquet`. The leftmost zero column tells you which join failed. |
| `f_{tag}` column is 1.0 for every row of an LOB | `forecast_factors.csv` has a different `office` string than `lobs.csv` | normalise the office string on either side. |
| Pipeline writes ZERO rows across all variants | `rollup_scope.csv` is empty or every row has `in_rollup=False` | populate it from `dim_region_perils.applies_to_*` per the SQL above. |
| Verisk EU flood rows show up in the AIR fanout (should be in RMS) | `perils.csv` has wrong `peril_family` for the flood peril (must be exactly `"FL"`) | fix the `peril_family` value to `"FL"`. |
