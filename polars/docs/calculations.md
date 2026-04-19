# Calculations — january (duckdb) → polars

Every calc that lives in `jan-rollup/duckdb_schema/view_definitions.csv`,
mapped to the polars stage that replaces it.

Notation:

- **duckdb object** `schema.view_or_table` — the duckdb definition.
- **polars** — the stage module + function that replaces it.
- **status** — `done` (real math) / `stub` (placeholder math, runs end-to-end) / `todo`.

Column-name convention: january used the wire names (`yearid`, `eventid`,
`"Rate to GBP"`, etc.); polars canonicalises these at the staging boundary into
snake_case (`year_id`, `event_id`, `rate_to_gbp`). See `rollup/schemas/columns.py`.

---

## 0. Inputs

| january                                 | polars                                 | status |
| --------------------------------------- | -------------------------------------- | ------ |
| `stg_rl_ylt` (RiskLink YLT)             | `stages.staging.load_raw_risklink_ylt` | done   |
| `stg_vk_ylt` (Verisk YLT)               | `stages.staging.load_raw_verisk_ylt`   | done   |
| `stg_rl_ep`, `stg_vk_ep` (EP summaries) | `stages.staging.load_ep_summaries`     | todo   |
| `reference.*` seeds                     | `rollup.seeds.load_all`                | done   |

January used **RiskLink YLTs from a DocDB dump**; we are now using **AIR
simulation YLTs** (`jan-rollup/air_ylt_c1.parquet` + `air_ylt_c2.parquet`).
Those two files are halves of one dataset — `pl.scan_parquet([c1, c2])`
(or `pl.scan_parquet("jan-rollup/air_ylt_c*.parquet")`) concatenates them
transparently.

EP summaries are currently in excel (`jan-rollup/ep_summaries/rms_ep_summary.xlsx`
etc.). These feed `rl_proportion` / `vk_proportion` in blending — required,
not optional. They need to be converted into a normalised tabular form
with columns matching `stg_rl_ep` / `stg_vk_ep` (rp, ep_type, lob,
region_peril, gl).

---

## 1. YLT staging

### 1.1 `int_vw_rl_ylt` → `stages.staging.normalize_risklink_ylt` — **done**

duckdb:
```sql
SELECT lobs.id AS lob_id, lobs.modelled_lob, lobs.rollup_lob, lobs.lob_type,
       lobs.cds_cat_class_name,
       rps.id AS region_peril_id, rps.modelled_region_peril, rps.cleaned_region_peril,
       rps.rollup_region_peril,
       yearid, eventid, loss
FROM stg_rl_ylt
INNER JOIN dim_rl_analysis dra  ON dra.rl_analysis_id = stg_rl_ylt.anlsid
INNER JOIN dim_region_perils rps ON rps.modelled_region_peril = dra.region_peril
INNER JOIN reference.lobs lobs   ON lobs.modelled_lob = dra.lob;
```

polars: `stages/staging.py::normalize_risklink_ylt`. Same triple join, with right-side
frames pre-selected so their `id` columns are aliased before the join (avoids
`dim_region_perils.id` colliding with `lobs.id`).

### 1.2 `int_vw_vk_ylt` → `normalize_verisk_ylt` — **done**

duckdb:
```sql
SELECT lobs.id AS lob_id, ..., model_code, yearid, eventid,
       net_pre_cat_loss AS loss
FROM stg_vk_ylt stg
INNER JOIN reference.lobs lobs       ON lobs.modelled_lob = stg.lob
INNER JOIN dim_region_perils rps     ON rps.modelled_region_peril = stg.analysis
WHERE rps.vendor = 'verisk' AND catalog_type_code = 'STC';
```

polars: `stages/staging.py::normalize_verisk_ylt`. Mirrors RL but joins on
`Analysis` (the raw parquet column) → `dim_region_perils.modelled_region_peril`
for vendor='verisk' rows, and filters `CatalogTypeCode='STC'`. `MODEL_CODE`
comes straight from the raw parquet's `ModelCode` column.

### 1.3 YLT union + ranking (`int_vw_funnel_ylt_combined_ranked*`) — **partial**

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

polars: the **union** is done via `pl.concat([rl_norm, vk_norm])` inside
`build_all_factors`. The **ranking** is `stages/factors.py::attach_rank`,
which produces the `rnk` column that `attach_euws` needs for the
HIC_HH_UK special case.

The `rp` / `rp_bucket` numeric bucketing is **todo** — not needed yet
because the fan-out currently filters by vendor, not by rp_bucket. Sim
counts (10 000 / 100 000) live on `Vendor.n_simulations` in
`rollup/config.py`.

### 1.4 Validity filter (`int_vw_analysis_is_valid` + `..._valid`) — **todo**

duckdb keeps only (lob_id, region_peril_id) pairs that have an AAL row
in `vw_ep` AND `official_rollup = 1`:

```sql
SELECT DISTINCT vendor, lob_id, region_peril_id, official_rollup
FROM vw_ep
WHERE ep_type='AAL' AND official_rollup=1;
```

polars: once `rollup_scope.csv` is populated (see §9 + `RH-TODO-DATA.md`),
this becomes an inner join of the YLT against the rollup-scope seed. See
§9 for the current schema and the variant-selection issue this filter
addresses.

---

## 2. EP summary staging

### 2.1 `vw_ep` — **todo**

Unions RL + VK EP summaries and enriches with `lobs`, `dim_region_perils`,
plus the computed `official_rollup` flag:

```sql
SELECT 'risklink' AS vendor, rp, ep_type, modelled_lob, rollup_lob, lob_type,
       modelled_region_peril, ..., rollup_region_peril, region, peril,
       CASE lob_type
         WHEN 'mga'  THEN applies_to_mga
         WHEN 'prop' THEN applies_to_prop
         WHEN 'fa'   THEN applies_to_fa
         ELSE 0
       END AS official_rollup,
       rp.id AS region_peril_id, lobs.id AS lob_id,
       blending_factor_region_peril_id, blending_factor_sub_region_peril_id,
       cds_cat_class_name, gl
FROM stg_rl_ep INNER JOIN modelled_lobs ... INNER JOIN region_perils ...
UNION ALL
SELECT 'verisk' AS vendor, ... FROM stg_vk_ep ...;
```

polars: `stages.ep_summary.build_vw_ep(rl_ep, vk_ep, lobs, dim_region_perils)`.
`official_rollup` → `pl.when(lob_type=='mga').then(applies_to_mga)...otherwise(0)`.

---

## 3. Blending

### 3.1 `int_vw_blending__vendor_proportions_all_rps_pre_factors`

Self-join rl+vk on (rollup_lob, rollup_region_peril, ep_type, rp); produces
the proportions used to blend:

```sql
WITH rl AS (...), vk AS (...)
SELECT ...,
       (rl.gl / (rl.gl + vk.gl)) AS rl_proportion,
       (vk.gl / (rl.gl + vk.gl)) AS vk_proportion,
       (rl.gl / vk.gl)            AS ratio_to_verisk
FROM rl INNER JOIN vk
  ON rl.rollup_lob = vk.rollup_lob
 AND rl.rollup_region_peril = vk.rollup_region_peril
 AND rl.ep_type = vk.ep_type
 AND rl.rp = vk.rp
WHERE official_rollup=1 AND ep_type IN ('AAL','OEP')
  AND rp IN (0, 200, 1000, 10000);
```

polars: `stages.blending.vendor_proportions(vw_ep)` — **todo**.
One frame, `.filter(vendor=='risklink')` and `.filter(vendor=='verisk')`, then
inner join + arithmetic. Currently stubbed as a pass-through in
`attach_uplift` (rl_proportion=vk_proportion=0.5, uplift=1.0).

### 3.2 `int_vw_blending_factors_applied`

Joins `reference.blending_factors` and computes the uplift factor:

```
rl_blended_contribution = COALESCE(rl_loss,1) * RMSBlend
vk_blended_contribution = COALESCE(vk_loss,1) * AIRBlend
blended_target_loss     = rl_blended_contribution + vk_blended_contribution
base_model              = if rollup_region_peril IN ('EU_FL','UK_FL') then 'risklink' else 'verisk'
base_model_loss         = if base_model='risklink' then rl_loss else vk_loss
uplift_factor_on_base_model        = blended_target_loss / base_model_loss
uplift_factor_on_base_model_capped = CLAMP(uplift, 0.1, 10.0)
```

polars: `stages.blending.apply_blending_factors(props, blending_factors)` —
**todo**. Clamp will be `pl.col.uplift.clip(lower_bound=0.1, upper_bound=10.0)`.
Currently `attach_uplift` stubs this as a pass-through (uplift_factor=1.0,
uplift_factor_capped=1.0, base_model=vendor).

The two EU_FL / UK_FL rollup_region_perils use RiskLink as base because
Verisk doesn't model European flood (per the schema author's note from
january).

### 3.3 `int_vw_blending_factors_with_forecast` — **done**

Joins `reference.forecast_factors_with_lobs_to_apply` on `lob_id`, bringing
the `f_{yyyymm}` forecast factors. `COALESCE(_, 1.0)` so lobs without
a mapped forecast factor are pass-through.

january **parses `rollup_lob` by `_`** to derive `office` and `class`:

```sql
rollup_lob_split = split(rollup_lob, '_')
office = rollup_lob_split[last]
class  = if len>1 then rollup_lob_split[2] else rollup_lob_split[1]
```

polars: **`office` and `class` are pre-computed columns on `lobs.csv`** —
the runtime parse is skipped. `stages/factors.py::attach_forecast_factors`
joins on `(office, class)` directly.

The forecast **dates** are data-driven from `forecast_factors.csv` — not
hardcoded `f_202601/202607/202701` as in january. Add a row with a new
`forecast_date` to the seed and a new `f_{yyyymm}` column appears in the
output, a new set of metric columns, and new Hisco variants. No code change.

### 3.4 `int_vw_blending_factors_with_forecast_ccy` — **done**

Picks `required_currency` from `cds_cat_class_name`:

```sql
CASE WHEN cds_cat_class_name LIKE '% UK %' THEN 'GBP'
     WHEN cds_cat_class_name LIKE '% EU %' THEN 'EUR'
     ELSE 'GBP'  END AS required_currency
```

Joins `reference.fx_rates` filtered to `target_currency='GBP'`, attaches
`rate_to_gbp`.

polars: `stages/factors.py::attach_currency(ylt, fx_rates)`.

### 3.5 `int_vw_blending_factors_with_forecast_ccy_ylt_ready`

Just filters to `official_rollup=1 AND ep_type IN ('AAL','OEP')` — the
rows that can be joined onto bucketed YLT.

---

## 4. YLT × factors (the big join)

### 4.1 `mts_vw_ylt_combined_with_blending_factors_fx_applied`

Joins ranked/bucketed YLT against the forecast-and-ccy factors on:

```
base_model          = vendor
rollup_lob          = rollup_lob
rollup_region_peril = rollup_region_peril
rp                  = rp_bucket     -- factors use {0,200,1000,10000}
```

Then adds **5 derived metrics per row**:

```
original_ylt_loss                                 = loss
original_ylt_loss_uplifted                        = loss * uplift_factor_on_base_model
original_ylt_loss_uplifted_capped                 = loss * uplift_factor_on_base_model_capped
original_ylt_loss_uplifted_capped_localccy        = above / rate_to_gbp
original_ylt_loss_uplifted_capped_localccy_202601 = above * f_202601
original_ylt_loss_uplifted_capped_localccy_202607 = above * f_202607
original_ylt_loss_uplifted_capped_localccy_202701 = above * f_202701
```

(6 derived metrics — 7 including the pass-through original.)

polars: `rollup.pipeline._compute_metrics(ylt, tags)` — **done** for the
year-tagged chain. Uplift + cap are still stubbed as pass-through (1.0)
until `stages/blending.py` lands.

### 4.2 `mts_vw_ylt_combined_with_blending_factors_fx_forecasted_euws_applied`

EUWS per-event adjustment. Builds a lookup from `air_events` +
`euws_rate_factors`:

```sql
WITH mcl AS (
  SELECT ae.EventID AS model_eventid, ae."Day", ae."Year", ae.ModelID,
         ae."Event" AS eventid,
         COALESCE(f.Factor, 1.0) AS Factor
  FROM reference.air_events ae
  LEFT JOIN reference.euws_rate_factors f ON ae.EventID = f.ModelEventID
)
```

then the special case:

```
euws_factor = CASE WHEN rollup_lob='HIC_HH_UK' AND rnk<=100 THEN 1.0
                   ELSE COALESCE(mcl.Factor, 1.0) END
```

and applies it to 4 more metrics:

```
original_ylt_loss_uplifted_capped_euws                 = capped * euws_factor
original_ylt_loss_uplifted_capped_localccy_euws        = localccy * euws_factor
original_ylt_loss_uplifted_capped_localccy_{year}_euws = localccy_{year} * euws_factor   (×3 years)
```

polars: `stages/factors.py::attach_euws(ylt, euws_rate_factors)` — **done**.

The special-case `HIC_HH_UK AND rnk<=100 → 1.0` is implemented with a
`pl.when().then().otherwise()` inside `attach_euws`. It only kicks in for
the top-100 UK household events; the rationale (from january) was that
euws factors aren't meaningful for the largest UK HH tail events.

Note: current polars implementation joins on `(event_id, year_id)` and
COALESCEs missing euws rows to 1.0 — it does NOT currently go through
`air_events` as an intermediate lookup (january did). Need the air_events
seed populated (see `RH-TODO-DATA.md`) before revisiting this to match
january exactly.

### 4.3 `mts_vw_ylt_combined_with_blending_factors_fx_forecasted_euws_applied_fagross`

Fine-art gross-to-net adjustment. Joins
`reference.fineart_gross_to_net_adjustment2` on (lob_id, rollup_region_peril),
then:

```
fa_gross_aal_factor  = COALESCE(aal_factor,  1.0)
fa_gross_tail_factor = COALESCE(tail_factor, 1.0)

original_ylt_loss_uplifted_capped_localccy_{year}_euws_fagross =
    CASE
      WHEN rp_bucket = 0    THEN localccy_{year}_euws * fa_gross_aal_factor
      WHEN rp_bucket >= 200 THEN localccy_{year}_euws * fa_gross_tail_factor
    END                              (×3 years)
```

polars: `stages/factors.py::attach_fagross(ylt, fineart_adjustments)` —
**partial**. Joins `fineart_adjustments` and attaches `fa_gross_aal_factor`
+ `fa_gross_tail_factor`. The current metrics loop multiplies by
`fa_gross_aal_factor` unconditionally — the `rp_bucket` split (aal_factor
for AAL, tail_factor for high RPs) is **todo** once the bucketing from
§1.3 lands.

### 4.4 `mts_tbl_ylt_combined_all_factors` (the cached DAG node) — **done**

duckdb materialises the output of 4.3 as a **BASE TABLE**. Every downstream
view reads from `..._from_cachetbl` variants to avoid recomputing. january's
readme notes this was added because recomputing the fan tree for 20+
downstream tables was too slow.

polars equivalent: `build_all_factors(cfg, seeds).cache()` in
`rollup/pipeline.py::run`. `.cache()` ensures the `LazyFrame` is computed
exactly once even when read by all 12 fan-out sinks + 2 audit dumps.

This is the `AllFactorsCol` + dynamic year-tagged metric columns node: the
wide table of dims + the three year-invariant MetricCol members + three
year-tagged metric columns per forecast tag + dialsup columns per tag.

---

## 5. Long-form + aggregation for fan-out

### 5.1 UNPIVOT to long form — **done** (audit parquet)

january: `mts_vw_ylt_combined_all_factors_long_from_cachetbl` unpivots the
wide cache into `(metric_name, value)` pairs.

polars: `rollup.pipeline.audit_long(all_factors, tags)` produces exactly
this shape. Written to `<output_dir>/debug/audit_long.parquet` when
`--dump-interim` is set. `metric_name` is a VALUE not an enum, so adding
more metrics (new forecast tags, new factors) adds new metric_name values
automatically.

### 5.2 Aggregation to Hisco grain — **not needed at current grain**

january groups to `(base_model, model_eventid, yearid, eventid, ccy, cds_cat_class_name, metric)`.

polars: no separate aggregation step — the wide `all_factors` frame is
already one row per YLT event (staging produced it that way), so
`fanout_hisco` projects directly without aggregating. If grouping becomes
necessary (e.g. multiple YLT rows per event_id appear), add a
`.group_by(...).agg(pl.sum(metric))` before `fanout_hisco`.

### 5.3 Fan-out to Hisco tables — **done**

polars: `rollup.pipeline.fanout_hisco(all_factors, variant)`. Filters by
`base_model == variant.vendor.name`, picks the `variant.loss_metric` column
as `ModelGrossLoss`, validates against `HISCO_FANOUT` schema, writes one
parquet per variant.

`ModelEventDay` — **still hardcoded to 0**. The real computation is:
- **AIR** variants: left-join `air_events` on `model_event_id = EventID`
  for `ModelEventDay = ae."Day"`.
- **RiskLink flood** variants: left-join `flood_rl22_model_events` on
  (ModelEventID, ModelYear = ModelOccurrenceYear), compute
  `ModelEventDay = date_part('doy', ModelOccurrenceDate)`.

Both joins need their seeds populated (see `RH-TODO-DATA.md`).

### 5.4 Flavors

No per-variant SQL flavour mess like january's `_fix` / `_fl_fa_fix` /
`_domestic_euws_fix`. polars has exactly two flavours:

- **`Flavor.MAIN`** — loss_metric = `loss_uplifted_capped_localccy_{tag}_euws_fagross`
- **`Flavor.DIALSUP`** — loss_metric = `dialsup_{tag}`

fa_gross is a factor, not a flavour. See `rollup/config.py::Flavor` for
the rationale.

---

## 6. EP curves (`mts_vw_ep_combined_all_factors*`)

Three flavours (overall / by_cds_class / by_lob / copilot), all with the
same pattern:

```
per_year(key, yearid) = sum(value)       -- for AEP
                      | max(value)       -- for OEP
rnk = row_number(per_year) order by value desc, partition by key
AAL = CASE base_model
        WHEN 'risklink' THEN sum(value)/100000.0
        WHEN 'verisk' THEN sum(value)/10000.0
      END
rp  = CASE base_model
        WHEN 'risklink' THEN 100000/rnk
        WHEN 'verisk' THEN 10000/rnk
      END
```

Filters to rnk ∈ {0, 10, 100, 500, 1000, 2000, 5000} for rl and
{0, 1, 10, 50, 100, 200, 500} for vk (0 = AAL row).

polars: `stages.ep.ep_curve_from_ylt` — **done** (generalised to any
`n_simulations`; defaults use `DEFAULT_RETURN_PERIODS`). The per-vendor
return-period sets are what fall out of `n=10000` and `n=100000` when you
enumerate integer `n/rnk`. We should confirm those two vendor RP sets match
`DEFAULT_RETURN_PERIODS` after `n_simulations` is set correctly per vendor.

---

## 7. Dials-up funnel (`mts_vw_ylt_dialsup__funnel`) — **done**

The DIALSUP flavour computes per-event ratios of the fully-factored metric
to the localccy baseline, then applies those ratios back to `loss_raw`:

```
f_ratio_{tag} = loss_uplifted_capped_localccy_{tag}_euws_fagross
              / loss_uplifted_capped_localccy
dialsup_{tag} = f_ratio_{tag} × loss_raw
```

polars: `rollup.pipeline._compute_dialsup(ylt, tags)`. One `dialsup_{tag}`
column per forecast tag, handling divide-by-zero via `pl.when`. The
`Flavor.DIALSUP` variant picks this column as its `ModelGrossLoss`.

---

## 8. Verify views (`verify.*`)

Invariants asserted after each stage. Worth reproducing as pytest fixtures
rather than views:

| view                                               | invariant                                                           |
| -------------------------------------------------- | ------------------------------------------------------------------- |
| `check_aal_pre_euws`                               | sum(metric)/n_sims equals AAL for pre-euws metrics                  |
| `check_aal_after_all_factors`                      | same, for all 12 all-factors metrics                                |
| `forecast_factors_missing_lobs`                    | every lob in forecast_factors maps to a known rollup_lob            |
| `lobs_with_forecast_factors_not_in_reference_lobs` | inverse direction                                                   |
| `rl_staging_aal_equals_rl_intermediate_aal`        | sum(stg_rl_ylt.loss)/100000 == sum(int_vw_rl_ylt.loss)/100000       |
| `verisk_ylt_analysis_not_in_dim_region_perils`     | every `stg_vk_ylt.analysis` is a known modelled_region_peril       |
| `vw_ep_blending_factor_id_is_in_blending_factor_table` | every vw_ep blending_factor_region_peril_id exists in blending_factors |

polars: `tests/test_invariants.py` — **todo**. One started:
`count_event_id_orphans` in `rollup/pipeline.py` counts YLT rows whose
`(year_id, event_id, model_code)` triple is not in `air_events`. Logs a
warning and returns the count; observation-only by design (the rollup math
doesn't depend on `air_events`).

---

## 9. Official rollup selection

### 9.1 How `official_rollup` is computed

`vw_ep` produces the `official_rollup` column using a CASE on `lob_type`,
pulling one of the three `applies_to_*` flags from `dim_region_perils`:

```sql
-- from vw_ep (loader.main)
CASE
  WHEN lob_type = 'mga'  THEN applies_to_mga
  WHEN lob_type = 'prop' THEN applies_to_prop
  WHEN lob_type = 'fa'   THEN applies_to_fa
  ELSE 0
END AS official_rollup
```

The flag value is always `0` or `1`. So `official_rollup = 1` means "this
(vendor, modelled_region_peril, lob_type) combination is in scope for the
rollup." Downstream, every view that filters `official_rollup = 1` (e.g.
`int_vw_analysis_is_valid`, `int_vw_blending__vendor_proportions_*`,
`int_vw_blending_factors_with_forecast_ccy_ylt_ready`) relies entirely on
these per-row flags in `dim_region_perils`.

### 9.2 Where variant selection lives

The "pick one Verisk variant" logic is **encoded directly in the
`applies_to_*` flag values on each `dim_region_perils` row.** There is no
separate decision table.

Consider Europe / UK Winter Storm (peril_id 206), which has four Verisk
rows in `dim_region_perils`:

| `modelled_region_peril` | `applies_to_mga` | `applies_to_prop` | `applies_to_fa` |
|-------------------------|-----------------|------------------|-----------------|
| `EU_WS`                 | 0               | 0                | 0               |
| `EU_WS_GCAdj`           | 1               | 1                | 1               |
| `UK_WSSS`               | 0               | 0                | 0               |
| `UK_WSSS_GCAdj`         | 1               | 1                | 1               |

(Values inferred from the fact that `_GCAdj` is the gust-corrected variant
selected for the rollup; actual values must be confirmed from the duckdb
export.) Only the `_GCAdj` rows will survive `WHERE official_rollup = 1`.
The plain `UK_WSSS` and `EU_WS` rows have `applies_to_*=0` for all LOB
types and are effectively dead in the pipeline.

Crucially, **two analyses can share the same `peril_id`** (e.g. both
`UK_WSSS` and `UK_WSSS_GCAdj` map to peril 206), but at most one will have
`applies_to_*=1` for any given lob_type. The flag is set at the
`modelled_region_peril` grain, not at the `peril_id` grain.

### 9.3 Implication for `rollup_scope` schema

The current `rollup_scope(lob_id, peril_id, in_rollup)` schema is
**insufficient** because two analyses can share a `peril_id`. For example,
setting `in_rollup=1` for `(lob_id=3, peril_id=206)` doesn't say which of
`UK_WSSS` vs `UK_WSSS_GCAdj` is the live analysis.

The correct grain is `(lob_id, analysis_id, in_rollup)`, where `analysis_id`
is the key into `analyses.csv` (the `modelled_label` / `analysis_id` column
for Verisk, or the `rl_analysis_id` for RiskLink). This matches the grain of
`dim_region_perils` one-for-one.

**Decision**: change `rollup_scope.csv` schema to
`(lob_id, vendor, analysis_id, in_rollup)`. The SQL to populate it is a
CROSS JOIN of `reference.lobs × dim_region_perils` with the CASE flag as
`in_rollup` — see `RH-TODO-DATA.md` for the export query.

---

## 10. Reference-data source of truth

Two overlapping sources exist:

### 10.1 `jan-rollup/duckdb_schema` (january)

Authoritative for the january rollup run. Contains `dim_region_perils`,
`dim_rl_analysis`, `reference.air_events`, `reference.blending_factors`,
`reference.cds_region_peril`, `reference.euws_rate_factors`,
`reference.fineart_gross_to_net_adjustment2`, `reference.flood_rl22_model_events`,
`reference.forecast_factors`, `reference.fx_rates`, `reference.lobs`.

This is a schema dump only (table_definitions.csv); data rows aren't
checked in — they live in the duckdb db that generated the dump.

### 10.2 `dbt/seeds` (polars project, currently)

Only 5 files:
- `fx-rates/fx_rates.csv`                        — (base_ccy, target_ccy, rate) — **shape differs from january**
- `hisco-org/hisco_org__forecast_factors.csv`    — long format (class, office, office_iso2, base_date, forecast_date, forecast_factor) — **shape differs from january's wide f_202601/f_202607/f_202701**
- `hisco-org/hisco_org__lobs.csv`                — matches january `reference.lobs` + extra (office, class) columns
- `vor/vor_blending_factors.csv`                 — matches january `reference.blending_factors` (minus KatRiskBlend, DateCreated)
- `vor/vor_euws_rate_factors.csv`                — matches january `reference.euws_rate_factors` exactly

### 10.3 Gaps in dbt seeds vs january

Missing (i.e. nothing to load from dbt seeds):
- `dim_region_perils`         — critical; staging joins depend on this
- `dim_rl_analysis`           — critical; RL staging join key
- `air_events`                — needed for euws + ModelEventDay
- `cds_region_peril`          — used in some views
- `fineart_gross_to_net_adjustment2`  — needed for fa_gross stage
- `flood_rl22_model_events`   — needed for RL `ModelEventDay` in `fanout_rl_withdayid`

### 10.4 Recommendation

Short term: point polars at **january's duckdb database** (or a parquet
export of each reference table) as the source of truth. The five dbt seeds
either match (lobs, blending_factors, euws_rate_factors) or need a small
adapter (fx_rates, forecast_factors). Build one `load_seeds(seeds_dir)`
function in `stages/staging.py` that hides the choice of backing store.

Long term: decide whether dbt seeds are the canonical place to keep these
seven tables or whether we just extract from january's duckdb periodically.
The dbt seed CSVs are easier to review in PR diffs; the duckdb dump is
easier to keep in sync with whatever the analysis team is actually using.

---

## 11. Summary: stage → calc → file

| # | polars location                                      | calcs replaced                                                                        | status |
| - | ---------------------------------------------------- | ------------------------------------------------------------------------------------- | ------ |
| 1 | `stages/staging.py::normalize_{risklink,verisk}_ylt` | `int_vw_rl_ylt`, `int_vw_vk_ylt`                                                      | done   |
| 2 | `stages/ep_summary.py` (does not exist yet)          | `vw_ep`                                                                               | todo   |
| 3 | `stages/factors.py::attach_rank`                     | ranking part of `int_vw_funnel_ylt_combined_ranked*`                                  | done   |
| 3b| `rp_bucket` + validity filter                        | bucketing + `int_vw_analysis_is_valid`                                                | todo (needs rollup_scope populated) |
| 4 | `stages/blending.py` (not yet created)               | `int_vw_blending__vendor_proportions_*`, `..._applied`                                | todo (`attach_uplift` stubs with 1.0) |
| 5 | `stages/factors.py::attach_forecast_factors`         | `int_vw_blending_factors_with_forecast`                                               | done   |
| 5b| `stages/factors.py::attach_currency`                 | `int_vw_blending_factors_with_forecast_ccy`                                           | done   |
| 6 | `pipeline.py::_compute_metrics`                      | `mts_vw_ylt_combined_with_blending_factors_fx_applied` metric cascade                 | done (year-tagged); uplift is 1.0 stub |
| 7 | `stages/factors.py::attach_euws`                     | `..._fx_forecasted_euws_applied` incl. HIC_HH_UK rnk<=100 special case                | done   |
| 8 | `stages/factors.py::attach_fagross`                  | `..._fx_forecasted_euws_applied_fagross`                                              | partial (aal_factor applied; tail_factor split by rp_bucket = todo) |
| 9 | `pipeline.py::build_all_factors`                     | cache equivalent to `mts_tbl_ylt_combined_all_factors`                                | done (`.cache()`) |
|10 | `pipeline.py::fanout_hisco`                          | `marts.*fanout_air`, `*fanout_rl_nodayid`, `*fanout_rl_withdayid`                     | done (ModelEventDay still hardcoded 0 pending air_events + flood seeds) |
|11 | `stages/ep.py::ep_curve_from_ylt`                    | `mts_vw_ep_combined_all_factors*`                                                     | done (scalar, any `n_simulations`; used by integration tests) |
|12 | `pipeline.py::_compute_dialsup`                      | `mts_vw_ylt_dialsup__funnel` + fanouts                                                | done   |
|13 | `pipeline.py::count_event_id_orphans`                | part of `verify.*` — eventid orphan count (observation-only)                          | done   |
|13b| `tests/test_invariants.py`                           | the rest of `verify.*`                                                                | todo   |

**Overall**: end-to-end pipeline runs (see `tests/test_e2e.py`). Blending
uplift is the only "real math" stub; every other stage applies its factor
multiplicatively, falling back to 1.0 where reference data is missing.
