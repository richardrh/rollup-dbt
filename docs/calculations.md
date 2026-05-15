# Calculations — january (duckdb) → polars

Every calc that lives in `jan-rollup/duckdb_schema/view_definitions.csv`,
mapped to the polars stage that replaces it.

Notation:

- **duckdb object** `schema.view_or_table` — the duckdb definition.
- **polars** — the stage module + function that replaces it.
- **status** — `done` (real math) / `partial` (works but a piece is stubbed) / `todo`.

Column-name convention: january used the wire names (`yearid`, `eventid`,
`"Rate to GBP"`, etc.); polars canonicalises these at the staging boundary
into snake_case (`year_id`, `event_id`, `rate_to_gbp`). See
`rollup/schemas/columns.py`.

The peril dimension was **split out of the `dim_region_perils` god-table**
into focused seeds — `perils`, `analyses`, `valid_analyses`,
`blending_weights`. References below to "perils.csv etc." mean the new
split, not the legacy duckdb table. See
[`data-requirements.md`](data-requirements.md) for the export SQL.

---

## 0. Inputs

| january                                 | polars                                 | status |
| --------------------------------------- | -------------------------------------- | ------ |
| `stg_rl_ylt` (RiskLink YLT)             | `staging.ylt.load_raw_risklink_ylt`    | done   |
| `stg_vk_ylt` (Verisk YLT)               | `staging.ylt.load_raw_verisk_ylt`      | done   |
| `stg_rl_ep`, `stg_vk_ep` (EP summaries) | `staging.ep` / `io.ep_summary`         | todo   |
| `reference.*` seeds                     | `rollup.seeds.load_all`                | done   |

January used **RiskLink YLTs from a DocDB dump**; we are now using **AIR
simulation YLTs** (`data/ylt/verisk/air_ylt_c1.parquet` +
`air_ylt_c2.parquet`). Those two files are halves of one dataset —
`pl.scan_parquet([c1, c2])` (or
`pl.scan_parquet("data/ylt/verisk/air_ylt_c*.parquet")`) concatenates
them transparently.

EP summaries are currently delivered as Excel/long CSV inputs
(`data/ep_summaries/risklink/*.xlsx`, `*.long.csv`, etc.) for review. Blending
model shares come from the fixed reviewed `data/seeds/vor/blending_weights.csv`
seed, which is populated from the provided blending-factor table.

---

## 1. YLT staging

### 1.1 `int_vw_rl_ylt` → `staging.ylt.normalize_risklink_ylt` — **done**

duckdb (january, joined through `dim_region_perils` god-table):
```sql
SELECT lobs.id AS lob_id, lobs.modelled_lob, lobs.rollup_lob, lobs.lob_type,
       lobs.cds_cat_class_name,
       rps.id AS region_peril_id, rps.modelled_region_peril, rps.rollup_region_peril,
       yearid, eventid, loss
FROM stg_rl_ylt
INNER JOIN dim_rl_analysis dra  ON dra.rl_analysis_id = stg_rl_ylt.anlsid
INNER JOIN dim_region_perils rps ON rps.modelled_region_peril = dra.region_peril
INNER JOIN reference.lobs lobs   ON lobs.modelled_lob = dra.lob;
```

polars: `staging/ylt.py::normalize_risklink_ylt`. Three inner joins
through the **new split tables**:
```
raw.anlsid (Int64) → analyses.analysis_id (String, cast) → peril_id + lob_id
analyses.peril_id  → perils.peril_id → name + region + peril_family
analyses.lob_id    → lobs.lob_id     → office + lob_class + ...
```

For RiskLink, `analyses.lob_id` is populated (one analysis is 1:1 with
one (lob, peril)) so the lobs join is keyed by the analyses-supplied
`lob_id`, not by `modelled_lob` like Verisk.

### 1.2 `int_vw_vk_ylt` → `normalize_verisk_ylt` — **done**

duckdb (january):
```sql
SELECT lobs.id AS lob_id, ..., model_code, yearid, eventid,
       net_pre_cat_loss AS loss
FROM stg_vk_ylt stg
INNER JOIN reference.lobs lobs       ON lobs.modelled_lob = stg.lob
INNER JOIN dim_region_perils rps     ON rps.modelled_region_peril = stg.analysis
WHERE rps.vendor = 'verisk' AND catalog_type_code = 'STC';
```

polars: `staging/ylt.py::normalize_verisk_ylt`. Same shape but joins
through `analyses` (filtered to vendor='verisk') + `perils`. The numeric
`analysis_id` allow-list is applied first; raw AIR `Analysis` values then join
to `analyses.modelled_label`. For Verisk the analysis is peril-only
(`analyses.lob_id` is NULL) so lob is resolved via the YLT row's
`ExposureAttribute` → `lobs.modelled_lob`. Filters
`CatalogTypeCode='STC'`. `MODEL_CODE` comes straight from the raw
parquet's `ModelCode` column.

The `NormalizedYlt` schema carries `office` + `lob_class` (from the lobs
join) and `peril_name` + `region` + `peril_family` (from the perils join)
so downstream factor stages have semantic dims without re-joining.

### 1.3 YLT union + ranking (`int_vw_funnel_ylt_combined_ranked*`) — **done**

duckdb stitches the two vendors together then ranks losses within
(vendor, lob_id, region_peril_id):

```sql
WITH ylt AS (
    SELECT 'verisk' AS vendor, ..., loss FROM int_vw_vk_ylt
    UNION ALL
    SELECT 'risklink' AS vendor, ..., 0 AS model_code, ..., loss FROM int_vw_rl_ylt
)
SELECT row_number() OVER (
         PARTITION BY vendor, lob_id, region_peril_id
         ORDER BY loss DESC
       ) AS rnk,
       ...
FROM ylt;
```

polars: the **union** is `pl.concat([rl_norm, vk_norm])` inside
`rollup/pipeline.py::build_staging`. The **ranking** is
`intermediate/factors.py::attach_rank` — `pl.col(LOSS).rank(...).over([VENDOR,
LOB_ID, REGION_PERIL_ID])`. The `rnk` column feeds `attach_euws` which
applies rank-threshold overrides from `euws_rank_overrides.csv`.

Sim counts (10 000 / 100 000) live on `Vendor.n_simulations` in
`rollup/config.py`.

### 1.4 Validity filter (`int_vw_analysis_is_valid` + `..._valid`) → analysis scope — **done**

duckdb keeps only (lob_id, region_peril_id) pairs that have an AAL row
in `vw_ep` AND `official_rollup = 1`:

```sql
SELECT DISTINCT vendor, lob_id, region_peril_id, official_rollup
FROM vw_ep
WHERE ep_type='AAL' AND official_rollup=1;
```

polars: dry-run resolves an effective analysis scope from
`selected_analyses.csv` when present, falling back to legacy
`valid_analyses.csv` only when selected analyses are absent. The selected scope
is validated against analysis/peril metadata, converted EP summaries, and YLT
coverage before runtime.

Runtime reuses that same effective selection through `effective_analyses_for_run`
and `analysis_scope`. Verisk rows join through `analyses.modelled_label`; RiskLink
rows join through the selected numeric EP `ID` / `anlsid`.

---

## 2. EP summary staging

### 2.1 `vw_ep` — **todo (materialised view not required)**

January unioned RL + VK EP summaries upstream of the blending-factor application.
The polars run reads reviewed `blending_weights.csv` directly for model shares.
A materialised `vw_ep` equivalent is therefore not required in the main DAG.
The unioned `vw_ep` would still be useful for excel-diff QA — see
`tests/test_integration_ep.py`.

If/when it lands, the polars version should be a staging-layer model such as
`rollup.staging.ep_summary.build_vw_ep(rl_ep, vk_ep, lobs, perils)` —
`lobs` + `perils` replace the `dim_region_perils` join.

---

## 3. Blending — `attach_uplift` — **done**

january did this in three steps (`int_vw_blending__vendor_proportions_*`,
`int_vw_blending_factors_applied`, plus the
`int_vw_blending_factors_with_forecast*` chain). polars folds the
proportions + uplift into one stage and computes AAL with window
functions instead of group-by-then-rejoin.

### 3.1 Blend weights → `attach_uplift` — **done**

january keyed blending off `dim_region_perils.blending_factor_*_id` →
`reference.blending_factors.{air_blend, rms_blend}`. polars sources blend
weights directly from `blending_weights.csv` — wide-format
`(peril_id, return_period, sub_peril, base_model, verisk_weight, risklink_weight)`. The YLT
gets `rp = n_sim / rnk` and `rp_bucket` (`0`, `200`, `1000`, or `10000`) from
`attach_rank`, then `attach_uplift` joins on
`(region_peril_id, rp_bucket) → (peril_id, return_period)` and maps
`verisk_weight` / `risklink_weight` to `(vk_proportion, rl_proportion)`.

### 3.2 Uplift formula — **done**

january:
```
rl_blended_contribution = COALESCE(rl_loss,1) * RMSBlend
vk_blended_contribution = COALESCE(vk_loss,1) * AIRBlend
blended_target_loss     = rl_blended_contribution + vk_blended_contribution
base_model              = if rollup_region_peril IN ('EU_FL','UK_FL') then 'risklink' else 'verisk'
base_model_loss         = if base_model='risklink' then rl_loss else vk_loss
uplift_factor_on_base_model        = blended_target_loss / base_model_loss
uplift_factor_on_base_model_capped = CLAMP(uplift, 0.1, 10.0)
```

polars (`intermediate/factors.py::attach_uplift`): same formula but at AAL
grain (sum(loss) / `Vendor.n_simulations`), and computed with
**window functions** rather than group-by + join-back:

```python
(pl.when(pl.col(Y.VENDOR) == VendorName.VERISK)
   .then(pl.col(Y.LOSS)).otherwise(0.0)
   .sum().over([Y.LOB_ID, Y.REGION_PERIL_ID]) / n_sim_vk).alias("_vk_aal")
```

Conditional sum within `(lob_id, region_peril_id)`, broadcast to every
event row. No collapse, no rejoin. Fallback uplift = 1.0 when the base
model has no events for a group.

### 3.3 Base-model lookup — **done, seed-owned**

January matched `rollup_region_peril IN ('EU_FL', 'UK_FL')` — string
substring on a derived label. polars stores `base_model` in
`blending_weights.csv`; runtime uplift reads the provided seed per peril.

### 3.4 Forecast factors → `attach_forecast_factors` — **done**

january parsed `rollup_lob` by `_` to derive `office` + `class`. polars
**lobs.csv carries `office` + `class` directly**, and staging puts them
on the NormalizedYlt frame so `attach_forecast_factors` joins on
`(office, lob_class)` directly — no parse, no re-join. Forecast dates
are data-driven from `forecast_factors.csv`; add a row with a new
`forecast_date` and a new `f_{yyyymm}` column appears, the chain
extends, and new Hisco variants emit. No code change.

### 3.5 Currency → `attach_currency` — **done**

january:
```sql
CASE WHEN cds_cat_class_name LIKE '% UK %' THEN 'GBP'
     WHEN cds_cat_class_name LIKE '% EU %' THEN 'EUR'
     ELSE 'GBP'  END AS required_currency
```

polars: `intermediate/factors.py::attach_currency`. Same derivation, plus joins
`fx_rates.csv` (filtered to `target_currency='GBP'`) for `rate_to_gbp`.
**Raises `MissingFxRateError`** if any row needs a currency that
isn't in `fx_rates.csv` — silent `fill_null(1.0)` would have inflated
losses. Currency string values use `CurrencyCode` StrEnum
(`config.py`).

---

## 4. Year-tagged metric chain — `add_main_metrics` + `chain.CHAIN`

### 4.1 Year-invariant prelude — **done**

january: `mts_vw_ylt_combined_with_blending_factors_fx_applied` produces
`loss_uplifted`, `loss_uplifted_capped`, `loss_uplifted_capped_localccy`
(the `CHAIN_BASE`).

polars (`intermediate.metrics.add_main_metrics`):
```python
ylt = ylt.with_columns(
    (pl.col(Y.LOSS) * pl.col(AF.UPLIFT_FACTOR))       .alias(M.LOSS_UPLIFTED),
    (pl.col(Y.LOSS) * pl.col(AF.UPLIFT_FACTOR_CAPPED)).alias(M.LOSS_UPLIFTED_CAPPED),
).with_columns(
    (pl.col(M.LOSS_UPLIFTED_CAPPED) / pl.col(AF.RATE_TO_GBP)).alias(CHAIN_BASE),
)
```

### 4.2 Year-tagged chain — registry-driven — **done**

january wrote one more view per factor stage, ending at
`..._fx_forecasted_euws_applied`. polars walks the `chain.CHAIN` registry
once per tag:

```python
for tag in tags:
    prev = CHAIN_BASE
    for stage_name, stage in CHAIN.items():
        out = col_after(stage_name, tag)
        ylt = ylt.with_columns(
            (pl.col(prev) * pl.col(factor_col_for(stage, tag))).alias(out)
        )
        prev = out
```

`CHAIN` (in `rollup/chain.py`) is a `dict[str, ChainStage]` TypedDict:
```python
CHAIN = {
    "forecast": {"suffix": "",         "factor_col": "f_{tag}",                  "is_per_tag": True,  ...},
    "euws":     {"suffix": "_euws",    "factor_col": AF.EUWS_FACTOR,             "is_per_tag": False, ...},
}
```

Adding a new factor stage = one entry in CHAIN. `add_main_metrics`,
`_metric_cols_for`, `audit_wide`, and `VariantSpec.loss_metric` all
walk this registry — no other edits.

### 4.3 EUWS rank-threshold overrides — **done**

january hardcoded `CASE WHEN rollup_lob='HIC_HH_UK' AND rnk<=100 THEN 1.0`
inside the EUWS view. polars extracts this to
`euws_rank_overrides.csv` (one row per `(rollup_lob, max_rank, factor)`
override) so adding a new LOB override is a data-only change. The
`pl.when` lives inside `attach_euws`, fed by the seed.

### 4.4 Legacy gross-to-net adjustment — **removed**

The january-only gross-to-net adjustment is no longer part of the Polars
rollup. MAIN ends at `..._{tag}_euws`, and DIALSUP uses
`loss × forecast × EUWS`.

### 4.5 `mts_tbl_ylt_combined_all_factors` (the cached DAG node) — **done**

duckdb materialised the wide cache as a BASE TABLE so 20+ downstream
views could read it once.

polars equivalent: `build_intermediate(...).all_factors.cache()` in
`rollup/pipeline.py::run`. `.cache()` ensures the LazyFrame is computed
exactly once across all 12 fan-out sinks + 2 audit dumps.

---

## 5. Long-form + aggregation for fan-out

### 5.1 UNPIVOT to long form — **done** (audit parquet)

january: `mts_vw_ylt_combined_all_factors_long_from_cachetbl` unpivots
the wide cache into `(metric_name, value)` pairs.

polars: `rollup.pipeline.audit_long(all_factors, tags)` produces this
shape. Written to `<output_dir>/debug/audit_long.parquet` by default;
`--no-audit` skips the debug copy.

### 5.2 Aggregation to Hisco grain — **not needed at current grain**

january groups to
`(base_model, model_eventid, yearid, eventid, ccy, cds_cat_class_name, metric)`.
polars: `all_factors` is already at event grain so `fanout_hisco`
projects directly. Add a `.group_by(...).agg(...)` if multiple YLT rows
per event_id ever appear.

### 5.3 Fan-out to Hisco tables — **done**

polars: `rollup.pipeline.fanout_hisco(all_factors, variant)`. Filters
by `base_model == variant.vendor.name`, picks
`variant.loss_metric` (= `chain.main_loss_col(tag)` for MAIN,
= `chain.DIALSUP_COL` for DIALSUP) as `ModelGrossLoss`, validates
against `HISCO_FANOUT` schema, writes one parquet per variant.

`ModelEventDay` — **still hardcoded to 0**. Planned:
- **AIR** variants: left-join `air_events` on `model_event_id = EventID`
  for `ModelEventDay = ae.Day`.
- **RiskLink flood** variants: a separate flood-event seed (currently
  not in the SEEDS list) joined on (ModelEventID, ModelYear).

### 5.4 Flavors

No per-variant SQL flavour mess like january's `_fix` / `_fl_fa_fix` /
`_domestic_euws_fix`. polars has exactly two flavours:

- **`Flavor.MAIN`** — `loss_metric = chain.main_loss_col(tag)` =
  the LAST cumulative chain column.
- **`Flavor.DIALSUP`** — `loss_metric = chain.DIALSUP_COL`.

---

## 6. EP curves (`mts_vw_ep_combined_all_factors*`) — **done (auxiliary)**

Three flavours (overall / by_cds_class / by_lob), all with the same
pattern:

```
per_year(key, yearid) = sum(value)       -- for AEP
                      | max(value)       -- for OEP
rnk = row_number(per_year) order by value desc, partition by key
AAL = CASE base_model
        WHEN 'risklink' THEN sum(value)/100000.0
        WHEN 'verisk'   THEN sum(value)/10000.0
      END
rp  = CASE base_model
        WHEN 'risklink' THEN 100000/rnk
        WHEN 'verisk'   THEN 10000/rnk
      END
```

polars: `rollup.staging.ep.ep_curve_from_ylt`. Generalised to any
`n_simulations`; defaults use `DEFAULT_RETURN_PERIODS`. Used by
`tests/test_integration_ep.py` against the real Verisk YLT (gated on
parquets being present locally).

---

## 7. Dials-up sensitivity (`mts_vw_ylt_dialsup__funnel`) — **done**

```
dialsup = loss × forecast × EUWS
```

Single column (no per-tag emission) using the selected forecast tag. One file
per vendor.

polars: `rollup.intermediate.metrics.add_dialsup`. Produces a single `dialsup`
column from raw `loss`, the selected `f_{tag}` forecast factor, and
`euws_factor`.

---

## 8. Verify views (`verify.*`)

Invariants asserted after each stage. Worth reproducing as pytest
fixtures rather than views:

| view                                               | invariant                                                           |
| -------------------------------------------------- | ------------------------------------------------------------------- |
| `check_aal_pre_euws`                               | sum(metric)/n_sims equals AAL for pre-euws metrics                  |
| `check_aal_after_all_factors`                      | same, for all factor-chain metrics                                  |
| `forecast_factors_missing_lobs`                    | every lob in forecast_factors maps to a known rollup_lob            |
| `lobs_with_forecast_factors_not_in_reference_lobs` | inverse direction                                                   |
| `rl_staging_aal_equals_rl_intermediate_aal`        | sum(stg_rl_ylt.loss)/100000 == sum(int_vw_rl_ylt.loss)/100000       |
| `verisk_ylt_analysis_not_in_perils`                | every `stg_vk_ylt.analysis` is a known modelled_label in `analyses` |
| `vw_ep_blending_weight_present`                    | every (peril_id, return_period) target bucket has a blending_weights row |

polars: `tests/test_invariants.py` — **todo**. One started:
`count_event_id_orphans` in `rollup/pipeline.py` counts YLT rows whose
`(year_id, event_id, model_code)` triple is not in `air_events`. Logs
a warning and returns the count; observation-only by design (the rollup
math doesn't depend on `air_events`).

---

## 9. Official rollup selection — selected analyses — **done**

### 9.1 How january computed `official_rollup`

`vw_ep` produced the `official_rollup` column using a CASE on `lob_type`,
pulling one of the three `applies_to_*` flags from `dim_region_perils`:

```sql
CASE
  WHEN lob_type = 'mga'  THEN applies_to_mga
  WHEN lob_type = 'prop' THEN applies_to_prop
  WHEN lob_type = 'fa'   THEN applies_to_fa
  ELSE 0
END AS official_rollup
```

The "pick one Verisk variant" logic (e.g. UK_WSSS_GCAdj is in scope but
plain UK_WSSS isn't) was encoded entirely in these flag values, at the
`modelled_region_peril` grain — NOT at `peril_id` grain, because two
analyses can share a `peril_id`.

### 9.2 polars schema

Normal runs use `selected_analyses.csv` at grain `(vendor, analysis_id)` plus
`include`. Here `analysis_id` is the EP-summary identifier selected by the
analyst: RiskLink uses the numeric EP `ID` as text; Verisk uses the AIR
`Analysis` label. `analyses.csv` still resolves selections to canonical perils,
and `lobs.csv` resolves EP `modelled_lob` values.

`valid_analyses.csv` is legacy compatibility only. It is consulted only when
`selected_analyses.csv` is absent, so an empty or malformed legacy allow-list
does not block selected-analysis mode.

### 9.3 Population SQL

Maintain the analyst-selected EP-summary IDs for the run — see the
`selected_analyses.csv` section in
[`data-requirements.md`](data-requirements.md) for the
copy-pasteable contract.

### 9.4 Where it's applied

Dry-run builds an effective selection from `selected_analyses.csv` when present,
or from legacy `valid_analyses.csv` only when selected analyses are absent. It
validates the selected IDs against `analyses.csv` / `perils.csv`, converted EP
summaries, and YLT coverage.

Runtime uses the same effective selection through the `effective_analyses_for_run`
/ `analysis_scope` flow, so YLT staging and outputs use the dry-run-approved
scope.

---

## 10. Reference data — current source-of-truth

The seeds folder (`data/seeds/`) is the canonical store for
reference data the polars pipeline reads. See
[`data-requirements.md`](data-requirements.md) for shape, source, and
SQL to re-export from january's duckdb when refreshing.

| seed                       | populated by                                                   |
| -------------------------- | -------------------------------------------------------------- |
| `lobs.csv`                 | dbt (`hisco_org__lobs.csv`)                                    |
| `perils.csv`               | duckdb export from `loader.main.dim_region_perils` (DISTINCT)  |
| `analyses.csv`             | duckdb export — verisk + risklink rows                         |
| `selected_analyses.csv`    | analyst-owned EP-summary selection; authoritative when present |
| `valid_analyses.csv`       | legacy numeric allow-list fallback when selected analyses are absent |
| `blending_weights.csv`     | duckdb export — wide blend weights from `blending_factors`     |
| `forecast_factors.csv`     | dbt (`hisco_org__forecast_factors.csv`)                        |
| `fx_rates.csv`             | handcrafted (replace before prod)                              |
| `euws_rate_factors.csv`    | dbt (`vor__euws_rate_factors.csv`)                             |
| `euws_rank_overrides.csv`  | hand-curated                                                   |
| `verisk_events.parquet`    | parquet export from `reference.air_events`                     |
| `risklink_flood22_model_events.parquet` | parquet export from RiskLink event catalogue      |

Event catalogues are parquet-backed validation seeds. In normal analyst runs,
pre-flight validates `selected_analyses.csv` and does not block on legacy
`valid_analyses.csv` contents.

---

## 11. Summary: stage → calc → file

| # | polars location                                           | calcs replaced                                                                        | status |
| - | --------------------------------------------------------- | ------------------------------------------------------------------------------------- | ------ |
| 1 | `staging/ylt.py::normalize_{risklink,verisk}_ylt`         | `int_vw_rl_ylt`, `int_vw_vk_ylt`                                                      | done   |
| 2 | `effective_analyses_for_run` / `analysis_scope`           | `int_vw_analysis_is_valid` + `vw_ep`'s `official_rollup` CASE                         | done   |
| 3 | `intermediate/factors.py::attach_rank`                    | ranking part of `int_vw_funnel_ylt_combined_ranked*`                                  | done   |
| 4 | `intermediate/factors.py::attach_currency`                | `int_vw_blending_factors_with_forecast_ccy` (CCY derivation + FX join)                | done   |
| 5 | `intermediate/factors.py::attach_forecast_factors`        | `int_vw_blending_factors_with_forecast`                                               | done   |
| 6 | `intermediate/factors.py::attach_euws`                    | `..._fx_forecasted_euws_applied` incl. rank-threshold override (now seed-driven)      | done   |
| 7 | `intermediate/factors.py::attach_uplift`                  | `int_vw_blending__vendor_proportions_*` + `..._applied` + flood base-model            | done (window functions instead of group-by + join-back) |
| 8 | `intermediate/metrics.py::add_main_metrics` + `rollup/chain.py` | year-invariant + year-tagged metric cascade across `mts_vw_ylt_combined_*`      | done (registry-driven) |
| 9 | `pipeline.py::build_intermediate` / `build_marts`         | cache equivalent to `mts_tbl_ylt_combined_all_factors`                                | done (`.cache()`) |
|10 | `marts/hisco.py::fanout_hisco`                            | `marts.*fanout_air`, `*fanout_rl_nodayid`, `*fanout_rl_withdayid`                     | done (ModelEventDay still hardcoded 0 pending air_events join) |
|11 | `staging/ep.py::ep_curve_from_ylt`                        | `mts_vw_ep_combined_all_factors*`                                                     | done (used by integration tests, not the main rollup) |
|12 | `intermediate/metrics.py::add_dialsup`                    | `mts_vw_ylt_dialsup__funnel` + fanouts                                                | done   |
|13 | `pipeline.py::count_event_id_orphans`                     | part of `verify.*` — eventid orphan count (observation-only)                          | done   |
|13b| `tests/test_invariants.py`                                | the rest of `verify.*`                                                                | todo   |

**Overall**: end-to-end pipeline runs against synthetic data
(`tests/test_e2e.py`) producing 8 Hisco fanout parquets + the combined long-format parquet with non-zero
`ModelGrossLoss` values. The remaining gaps are (a) `ModelEventDay` join with
`air_events` / a flood-events seed and (b) reproducing the `verify.*` invariants as pytest
assertions.
