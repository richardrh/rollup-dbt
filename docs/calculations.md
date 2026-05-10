# Calculations — loss chain stages

Every computation step in the polars pipeline, mapped to its input/output.

Notation:

- **stage name** — the polars stage module + function.
- **reference SQL** — the equivalent logic in reference SQL (for understanding).
- **status** — `done` (real math) / `partial` (works but a piece is stubbed) / `todo`.

Column-name convention: the wire format uses names like `yearid`, `eventid`,
`"Rate to GBP"`; polars canonicalises these at the staging boundary
into snake_case (`year_id`, `event_id`, `rate_to_gbp`). See
`rollup/schemas/columns.py`.

The peril dimension is split into four single-purpose seeds — `perils`,
`analyses`, `rollup_scope`, `blending_weights`. These coherently represent
the peril cataloguing and scope information. See
[`data-requirements.md`](data-requirements.md) for the schemas.

---

## 0. Inputs

| input type         | polars module                          | status |
| -------------------- | -------------------------------------- | ------ |
| RiskLink YLT       | `stages.staging.load_raw_risklink_ylt` | done   |
| Verisk YLT         | `stages.staging.load_raw_verisk_ylt`   | done   |
| EP summaries       | `stages.staging.load_ep_summaries`     | todo   |
| Reference seeds    | `rollup.seeds.load_all`                | done   |

**YLTs (Year Loss Tables):** Verisk AIR and RiskLink simulation parquets
(`data/ylt/verisk/air_ylt_*.parquet` and `data/ylt/risklink/risklink_ylt_*.parquet`).
Multiple chunk files are concatenated transparently via glob patterns.

**EP summaries:** Optional, in long-format CSV (`data/ep_summaries/{vendor}/*.long.csv`).
The pipeline sources blend proportions directly from `blending_weights.csv` seed,
so EP summaries are not required for the main chain. They're used only by
integration tests for validation.

---

## 1. YLT staging

### 1.1 `stages.staging.normalize_risklink_ylt` — **done**

Reference SQL (join pattern):
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

polars: `stages/staging.py::normalize_risklink_ylt`. Three inner joins
through the **new split tables**:
```
raw.anlsid (Int64) → analyses.analysis_id (String, cast) → peril_id + lob_id
analyses.peril_id  → perils.peril_id → name + region + peril_family
analyses.lob_id    → lobs.lob_id     → office + lob_class + ...
```

For RiskLink, `analyses.lob_id` is populated (one analysis is 1:1 with
one (lob, peril)) so the lobs join is keyed by the analyses-supplied
`lob_id`, not by `modelled_lob` like Verisk.

### 1.2 `stages.staging.normalize_verisk_ylt` — **done**

Reference SQL (join pattern):
```sql
SELECT lobs.id AS lob_id, ..., model_code, yearid, eventid,
       net_pre_cat_loss AS loss
FROM stg_vk_ylt stg
INNER JOIN reference.lobs lobs       ON lobs.modelled_lob = stg.lob
INNER JOIN dim_region_perils rps     ON rps.modelled_region_peril = stg.analysis
WHERE rps.vendor = 'verisk' AND catalog_type_code = 'STC';
```

polars: `stages/staging.py::normalize_verisk_ylt`. Same shape but joins
through `analyses` (filtered to vendor='verisk') + `perils`. For Verisk
the analysis is peril-only (`analyses.lob_id` is NULL) so lob is resolved
via the YLT row's `ExposureAttribute` → `lobs.modelled_lob`. Filters
`CatalogTypeCode='STC'`. `MODEL_CODE` comes straight from the raw
parquet's `ModelCode` column.

The `NormalizedYlt` schema carries `office` + `lob_class` (from the lobs
join) and `peril_name` + `region` + `peril_family` (from the perils join)
so downstream factor stages have semantic dims without re-joining.

### 1.3 YLT union + ranking — **done**

Union the two vendors and rank losses within (vendor, lob_id, region_peril_id):

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
`rollup/pipeline.py::build_all_factors`. The **ranking** is
`stages/factors.py::attach_rank` — `pl.col(LOSS).rank(...).over([VENDOR,
LOB_ID, REGION_PERIL_ID])`. The `rnk` column feeds `attach_euws` which
applies rank-threshold overrides from `euws_rank_overrides.csv`.

Sim counts (10 000 / 100 000) live on `Vendor.n_simulations` in
`rollup/config.py`.

### 1.4 Validity filter — `apply_rollup_scope` — **done**

Keep only (lob_id, region_peril_id) pairs that are marked in_rollup=True:

```sql
SELECT DISTINCT vendor, lob_id, region_peril_id, official_rollup
FROM vw_ep
WHERE ep_type='AAL' AND official_rollup=1;
```

polars: `stages/staging.py::apply_rollup_scope`. Inner-joins the
post-staging YLT against `rollup_scope.csv` on
`(lob_id, vendor, modelled_region_peril)`, keeping only rows where
`in_rollup=True`.

The pre-flight `build_plan` reporter blocks the run when
`rollup_scope.csv` is empty (otherwise the inner join would silently drop
every YLT row) — see `seeds.REQUIRED_SEEDS`.

---

## 2. EP summary staging

### 2.1 `vw_ep` — **todo (now optional for main chain)**

January unioned RL + VK EP summaries to derive `rl_proportion` /
`vk_proportion` for blending. The polars pipeline sources blend
proportions directly from `blending_weights.csv` (long-format, see §3),
so `vw_ep` is no longer required for the main rollup. The unioned
`vw_ep` would still be useful for excel-diff QA — see
`tests/test_integration_ep.py`.

If/when it lands, the polars version would be
`stages.ep_summary.build_vw_ep(rl_ep, vk_ep, lobs, perils)` — `lobs` +
`perils` is the peril dimension lookup.

---

## 3. Blending — `attach_uplift` — **done**

Blending computes per-vendor losses then uplift-blends them. polars folds the
proportions + uplift into one stage and computes AAL with window functions.

### 3.1 Blend weights → `attach_uplift` — **done**

Blend weights come from `blending_weights.csv` — long-format
`(peril_id, sub_peril, vendor, weight)`. The join is
`ylt.region_peril_id → blending_weights.peril_id` filtered per vendor
(`verisk` / `risklink`), pivoted to `(vk_proportion, rl_proportion)`.

### 3.2 Uplift formula — **done**

Reference formula:
```
rl_blended_contribution = COALESCE(rl_loss,1) * RMSBlend
vk_blended_contribution = COALESCE(vk_loss,1) * AIRBlend
blended_target_loss     = rl_blended_contribution + vk_blended_contribution
base_model              = if rollup_region_peril IN ('EU_FL','UK_FL') then 'risklink' else 'verisk'
base_model_loss         = if base_model='risklink' then rl_loss else vk_loss
uplift_factor_on_base_model        = blended_target_loss / base_model_loss
uplift_factor_on_base_model_capped = CLAMP(uplift, 0.1, 10.0)
```

polars (`stages/factors.py::attach_uplift`): same formula but at AAL
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

### 3.3 Flood base-model rule — **done, semantic**

January matched `rollup_region_peril IN ('EU_FL', 'UK_FL')` — string
substring on a derived label. polars matches `peril_family == "FL"`
(joined from `perils.csv`) via the `config.FLOOD_FAMILY` constant. New
flood region in `perils.csv` → no code change.

### 3.4 Forecast factors → `attach_forecast_factors` — **done**

**lobs.csv carries `office` + `class` directly**, and staging puts them
on the NormalizedYlt frame so `attach_forecast_factors` joins on
`(office, lob_class)` directly — no parse, no re-join. Forecast dates
are data-driven from `forecast_factors.csv`; add a row with a new
`forecast_date` and a new `f_{yyyymm}` column appears, the chain
extends, and new Hisco variants emit. No code change.

### 3.5 Currency → `attach_currency` — **done**

**derivation:**
```
IF cds_cat_class_name contains 'UK' → GBP
IF cds_cat_class_name contains 'EU' → EUR
ELSE → GBP
```

polars: `stages/factors.py::attach_currency`. This derivation, plus joins
`fx_rates.csv` (filtered to `target_currency='GBP'`) for `rate_to_gbp`.
**Raises `MissingFxRateError`** if any row needs a currency that
isn't in `fx_rates.csv` — silent `fill_null(1.0)` would have inflated
losses. Currency string values use `CurrencyCode` StrEnum
(`config.py`).

---

## 4. Year-tagged metric chain — `_compute_metrics` + `chain.CHAIN`

### 4.1 Year-invariant prelude — **done**

Compute the base metric columns: `loss_uplifted`, `loss_uplifted_capped`, `loss_uplifted_capped_localccy` (the `CHAIN_BASE`).

polars (`pipeline._compute_metrics`):
```python
ylt = ylt.with_columns(
    (pl.col(Y.LOSS) * pl.col(AF.UPLIFT_FACTOR))       .alias(M.LOSS_UPLIFTED),
    (pl.col(Y.LOSS) * pl.col(AF.UPLIFT_FACTOR_CAPPED)).alias(M.LOSS_UPLIFTED_CAPPED),
).with_columns(
    (pl.col(M.LOSS_UPLIFTED_CAPPED) / pl.col(AF.RATE_TO_GBP)).alias(CHAIN_BASE),
)
```

### 4.2 Year-tagged chain — registry-driven — **done**

Walk the `chain.CHAIN` registry once per forecast tag, chaining factor multiplications:

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
    "fagross":  {"suffix": "_fagross", "factor_col": AF.FA_GROSS_AAL_FACTOR,     "is_per_tag": False, ...},
}
```

Adding a new factor stage = one entry in CHAIN. `_compute_metrics`,
`_metric_cols_for`, `audit_wide`, and `VariantSpec.loss_metric` all
walk this registry — no other edits.

### 4.3 EUWS rank-threshold overrides — **done**

january hardcoded `CASE WHEN rollup_lob='HIC_HH_UK' AND rnk<=100 THEN 1.0`
inside the EUWS view. polars extracts this to
`euws_rank_overrides.csv` (one row per `(rollup_lob, max_rank, factor)`
override) so adding a new LOB override is a data-only change. The
`pl.when` lives inside `attach_euws`, fed by the seed.

### 4.4 Fine-art `aal_factor` vs `tail_factor` — **partial**

Currently multiplies `aal_factor` unconditionally; `fa_gross_tail_factor` is
carried through to the audit dump but not applied. See the docstring on
`AllFactorsCol.FA_GROSS_TAIL_FACTOR`. Future: once the rp_bucket split lands
the chain can branch conditionally on the output bucket.

### 4.5 Cache the combined all-factors node — **done**

The wide all-factors node is referenced by 12 fan-out sinks and 2 audit dumps.

polars: `build_all_factors(cfg, seeds).cache()` in
`rollup/pipeline.py::run`. `.cache()` ensures the LazyFrame is computed
exactly once across all sinks + audits.

---

## 5. Long-form + aggregation for fan-out

### 5.1 UNPIVOT to long form — **done** (audit parquet)

Unpivot the wide cache into `(metric_name, value)` pairs.

polars: `rollup.pipeline.audit_long(all_factors, tags)` produces this
shape. Written to `<output_dir>/debug/audit_long.parquet` when
`--dump-interim` is set.

### 5.2 Aggregation to Hisco grain — **not needed at current grain**

`all_factors` is already at event grain so `fanout_hisco`
projects directly. Add a `.group_by(...).agg(...)` if multiple YLT rows
per event_id ever appear.

### 5.3 Fan-out to Hisco tables — **done**

polars: `rollup.pipeline.fanout_hisco(all_factors, variant)`. Filters
by `base_model == variant.vendor.name`, picks
`variant.loss_metric` (= `chain.main_loss_col(tag)` for MAIN,
= `chain.dialsup_col(tag)` for DIALSUP) as `ModelGrossLoss`, validates
against `HISCO_FANOUT` schema, writes one parquet per variant.

`ModelEventDay` — **still hardcoded to 0**. Planned:
- **AIR** variants: left-join `air_events` on `model_event_id = EventID`
  for `ModelEventDay = ae.Day`.
- **RiskLink flood** variants: a separate flood-event seed (currently
  not in the SEEDS list) joined on (ModelEventID, ModelYear).

### 5.4 Flavors

Polars has exactly two flavours:

- **`Flavor.MAIN`** — `loss_metric = chain.main_loss_col(tag)` =
  the LAST cumulative chain column.
- **`Flavor.DIALSUP`** — `loss_metric = chain.dialsup_col(tag)`.

`fa_gross` is a chain factor, not a flavour. See `rollup/config.py::Flavor`.

---

## 6. EP curves — **done (auxiliary)**

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

polars: `stages.ep.ep_curve_from_ylt`. Generalised to any
`n_simulations`; defaults use `DEFAULT_RETURN_PERIODS`. Used by
`tests/test_integration_ep.py` against the real Verisk YLT (gated on
parquets being present locally).

---

## 7. Dials-up sensitivity (`mts_vw_ylt_dialsup__funnel`) — **done**

```
dialsup = loss / rate_to_gbp
```

Currency conversion only — no factors. Single column (no per-tag emission),
because the formula is tag-independent. One file per vendor.

polars: `rollup.pipeline._compute_dialsup`. Produces a single `dialsup` column
by dividing raw `loss` by `rate_to_gbp`. Divide-by-zero guard
(`pl.when(... != 0)`) returns 0.0 — documented in the function docstring.

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
| `vw_ep_blending_weight_present`                    | every (peril_id, vendor) in vw_ep has a blending_weights row        |

polars: `tests/test_invariants.py` — **todo**. One started:
`count_event_id_orphans` in `rollup/pipeline.py` counts YLT rows whose
`(year_id, event_id, model_code)` triple is not in `air_events`. Logs
a warning and returns the count; observation-only by design (the rollup
math doesn't depend on `air_events`).

---

## 9. Official rollup selection — `rollup_scope.csv` — **done**

`rollup_scope.csv` determines which (lob, vendor, analysis) combinations are
officially in scope. The "pick one Verisk variant" logic (e.g. UK_WSSS_GCAdj
is in scope but plain UK_WSSS isn't) is encoded at the `modelled_region_peril`
grain in the `analysis_id` column — not at `peril_id` grain, because two
analyses can share a `peril_id`.

### 9.1 Schema

`rollup_scope.csv` (in the new four-way split) has grain
`(lob_id, vendor, analysis_id, in_rollup)` where `analysis_id` is the
**modelled label** (matches `analyses.modelled_label` and the YLT's
`MODELLED_REGION_PERIL` after staging) — explicitly chosen to
distinguish analyses that share a peril_id.

### 9.2 Population

See the `rollup_scope.csv` section in
[`data-requirements.md`](data-requirements.md) for population guidance.

### 9.3 Where it's applied

`stages/staging.py::apply_rollup_scope`, called in
`rollup/pipeline.py::build_all_factors` immediately after staging:
```python
ylt = ylt.pipe(apply_rollup_scope, seeds.rollup_scope)
```

Inner-join keyed on `(lob_id, vendor, modelled_region_peril)` keeping
`in_rollup=True` rows. The pre-flight `build_plan` blocks runs when
`rollup_scope` is empty (would silently drop every row).

---

## 10. Reference data — current source-of-truth

The seeds folder (`data/seeds/`) is the canonical store for
reference data the polars pipeline reads. Eleven seeds total; see
[`data-requirements.md`](data-requirements.md) for shape and source.

| seed                       | populated by                                                   |
| -------------------------- | -------------------------------------------------------------- |
| `lobs.csv`                 | dbt (`hisco_org__lobs.csv`)                                    |
| `perils.csv`               | peril dimension export (DISTINCT by peril_id)                  |
| `analyses.csv`             | vendor-to-peril mapping (verisk + risklink rows)               |
| `rollup_scope.csv`         | rollup scope matrix (lob × vendor × analysis → in_rollup)     |
| `blending_weights.csv`     | blend weights by peril + vendor (long format)                  |
| `forecast_factors.csv`     | dbt (`hisco_org__forecast_factors.csv`)                        |
| `fx_rates.csv`             | handcrafted (replace before prod)                              |
| `euws_rate_factors.csv`    | dbt (`vor__euws_rate_factors.csv`)                             |
| `euws_rank_overrides.csv`  | hand-curated                                                   |
| `air_events.csv`           | Verisk event catalogue (recommended)                           |
| `fineart_adjustments.csv`  | fine-art adjustment factors (optional)                         |

Four of these are stub-empty in git, awaiting user export. The pre-flight
reporter blocks the run if any `REQUIRED_SEEDS` have zero rows.

---

## 11. Summary: stage → calc → file

| # | polars location                                           | calcs replaced                                                                        | status |
| - | --------------------------------------------------------- | ------------------------------------------------------------------------------------- | ------ |
| 1 | `stages/staging.py::normalize_{risklink,verisk}_ylt`      | `int_vw_rl_ylt`, `int_vw_vk_ylt`                                                      | done   |
| 2 | `stages/staging.py::apply_rollup_scope`                   | `int_vw_analysis_is_valid` + `vw_ep`'s `official_rollup` CASE                         | done   |
| 3 | `stages/factors.py::attach_rank`                          | ranking part of `int_vw_funnel_ylt_combined_ranked*`                                  | done   |
| 4 | `stages/factors.py::attach_currency`                      | `int_vw_blending_factors_with_forecast_ccy` (CCY derivation + FX join)                | done   |
| 5 | `stages/factors.py::attach_forecast_factors`              | `int_vw_blending_factors_with_forecast`                                               | done   |
| 6 | `stages/factors.py::attach_euws`                          | `..._fx_forecasted_euws_applied` incl. rank-threshold override (now seed-driven)      | done   |
| 7 | `stages/factors.py::attach_fagross`                       | `..._fx_forecasted_euws_applied_fagross` (aal_factor)                                 | partial (tail_factor carried for audit, not applied — needs rp_bucket logic) |
| 8 | `stages/factors.py::attach_uplift`                        | `int_vw_blending__vendor_proportions_*` + `..._applied` + flood base-model            | done (window functions instead of group-by + join-back) |
| 9 | `pipeline.py::_compute_metrics` + `rollup/chain.py`       | year-invariant + year-tagged metric cascade across `mts_vw_ylt_combined_*`            | done (registry-driven) |
|10 | `pipeline.py::build_all_factors`                          | cache equivalent to `mts_tbl_ylt_combined_all_factors`                                | done (`.cache()`) |
|11 | `pipeline.py::fanout_hisco`                               | `marts.*fanout_air`, `*fanout_rl_nodayid`, `*fanout_rl_withdayid`                     | done (ModelEventDay still hardcoded 0 pending air_events join) |
|12 | `stages/ep.py::ep_curve_from_ylt`                         | `mts_vw_ep_combined_all_factors*`                                                     | done (used by integration tests, not the main rollup) |
|13 | `pipeline.py::_compute_dialsup`                           | `mts_vw_ylt_dialsup__funnel` + fanouts                                                | done   |
|14 | `pipeline.py::count_event_id_orphans`                     | part of `verify.*` — eventid orphan count (observation-only)                          | done   |
|14b| `tests/test_invariants.py`                                | the rest of `verify.*`                                                                | todo   |

**Overall**: end-to-end pipeline runs against synthetic data
(`tests/test_e2e.py`) producing 8 Hisco fanout parquets + the combined long-format parquet with non-zero
`ModelGrossLoss` values. The remaining gaps are (a) `ModelEventDay` join with
`air_events` / a flood-events seed, (b) `fa_gross_tail_factor`
application, (c) reproducing the `verify.*` invariants as pytest
assertions.
