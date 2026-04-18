# Calculations — january (duckdb) → polars

Every calc that lives in `jan-rollup/duckdb_schema/view_definitions.csv`,
mapped to the polars stage that will replace it.

Notation:

- **duckdb object** `schema.view_or_table` — the duckdb definition.
- **polars** — the stage module + function that replaces it.
- **status** — `done` / `stub` / `todo`.

Column-name convention: january used the wire names (`yearid`, `eventid`,
`"Rate to GBP"`, etc.); polars canonicalises these at the staging boundary into
snake_case (`year_id`, `event_id`, `rate_to_gbp`). See `rollup/schemas/columns.py`.

---

## 0. Inputs

| january                              | polars                               | status |
| ------------------------------------ | ------------------------------------ | ------ |
| `stg_rl_ylt` (RiskLink YLT)          | `stages.staging.load_raw_risklink_ylt` | done |
| `stg_vk_ylt` (Verisk YLT)            | `stages.staging.load_raw_vk_ylt`     | todo   |
| `stg_rl_ep`, `stg_vk_ep` (EP summaries) | `stages.staging.load_ep_summaries` | todo   |
| `reference.*` seeds                  | `stages.staging.load_seeds`          | todo (per-frame loaders exist conceptually, orchestrator missing) |

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

### 1.2 `int_vw_vk_ylt` → `normalize_vk_ylt` — **todo**

duckdb:
```sql
SELECT lobs.id AS lob_id, ..., model_code, yearid, eventid,
       net_pre_cat_loss AS loss
FROM stg_vk_ylt stg
INNER JOIN reference.lobs lobs       ON lobs.modelled_lob = stg.lob
INNER JOIN dim_region_perils rps     ON rps.modelled_region_peril = stg.analysis
WHERE rps.vendor = 'vk' AND catalog_type_code = 'STC';
```

polars: mirrors RL but joins on `analysis` (not `anlsid → rl_analysis_id`)
and filters `catalog_type_code = 'STC'`. `MODEL_CODE` comes from `stg_vk_ylt`
(not hard-coded `0`).

### 1.3 YLT union + ranking (`int_vw_funnel_ylt_combined_ranked*`)

duckdb stitches the two vendors together then ranks losses within
(vendor, lob_id, region_peril_id):

```sql
WITH ylt AS (
    SELECT 'vk' AS vendor, ..., loss FROM int_vw_vk_ylt
    UNION ALL
    SELECT 'rl' AS vendor, ..., 0 AS model_code, ..., loss FROM int_vw_rl_ylt
)
SELECT row_number() OVER (
         PARTITION BY vendor, lob_id, region_peril_id
         ORDER BY loss DESC
       ) AS rnk,
       ...
FROM ylt;
```

Then buckets:
```sql
CASE WHEN vendor='rl' THEN CAST(100000.0 / rnk AS INTEGER)
     WHEN vendor='vk' THEN CAST(10000.0  / rnk AS INTEGER) END AS rp,
CASE WHEN rp<200 THEN 0
     WHEN rp<1000 THEN 200
     WHEN rp<10000 THEN 1000
     ELSE 10000 END AS rp_bucket
```

polars: `stages.funnel.rank_and_bucket(ylt)` — **todo**. Use
`pl.concat([vk, rl])`, `pl.col("loss").rank(method="ordinal", descending=True).over([...])`,
then `when/then/otherwise` for bucketing.

Note: `100000` and `10000` are the simulation year counts for RL and VK
respectively. These should be parameters (`n_simulations_rl`, `n_simulations_vk`),
not magic numbers.

### 1.4 Validity filter (`int_vw_analysis_is_valid` + `..._valid`)

duckdb keeps only (lob_id, region_peril_id) pairs that have an AAL row
in `vw_ep` AND `official_rollup = 1`:

```sql
SELECT DISTINCT vendor, lob_id, region_peril_id, official_rollup
FROM vw_ep
WHERE ep_type='AAL' AND official_rollup=1;
```

polars: `stages.funnel.filter_valid(ranked, ep_frame)` — **todo**. Inner
join the ranked YLT against the distinct valid keys from the EP frame.

---

## 2. EP summary staging

### 2.1 `vw_ep` — **todo**

Unions RL + VK EP summaries and enriches with `lobs`, `dim_region_perils`,
plus the computed `official_rollup` flag:

```sql
SELECT 'rl' AS vendor, rp, ep_type, modelled_lob, rollup_lob, lob_type,
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
SELECT 'vk' AS vendor, ... FROM stg_vk_ep ...;
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
One frame, `.filter(vendor=='rl')` and `.filter(vendor=='vk')`, then inner
join + arithmetic.

### 3.2 `int_vw_blending_factors_applied`

Joins `reference.blending_factors` and computes the uplift factor:

```
rl_blended_contribution = COALESCE(rl_loss,1) * RMSBlend
vk_blended_contribution = COALESCE(vk_loss,1) * AIRBlend
blended_target_loss     = rl_blended_contribution + vk_blended_contribution
base_model              = if rollup_region_peril IN ('EU_FL','UK_FL') then 'rl' else 'vk'
base_model_loss         = if base_model='rl' then rl_loss else vk_loss
uplift_factor_on_base_model        = blended_target_loss / base_model_loss
uplift_factor_on_base_model_capped = CLAMP(uplift, 0.1, 10.0)
```

polars: `stages.blending.apply_blending_factors(props, blending_factors)`.
Clamp = `pl.col.uplift.clip(lower_bound=0.1, upper_bound=10.0)`.

The two EU_FL / UK_FL rollup_region_perils use RL as base because Verisk
doesn't model European flood (per the schema author's note from january).

### 3.3 `int_vw_blending_factors_with_forecast`

Joins `reference.forecast_factors_with_lobs_to_apply` on `lob_id`, bringing
`f_202601`, `f_202607`, `f_202701` — forecast factors at three future base
dates (Jan 2026, July 2026, Jan 2027). `COALESCE(_, 1.0)` so lobs without
a mapped forecast factor are passthrough.

`reference.forecast_factors_with_lobs_to_apply` is itself a view over
`reference.lobs_with_class_office`, which **parses `rollup_lob` by `_`**
to derive `office` and `class`:

```sql
rollup_lob_split = split(rollup_lob, '_')
office = rollup_lob_split[last]
class  = if len>1 then rollup_lob_split[2] else rollup_lob_split[1]
```

Join keys then are (office == office_iso2, class == class).

polars: `stages.forecast.join_forecast_factors(blended, forecast_factors, lobs)`
— **todo**. Implement the `rollup_lob` split with `pl.col.rollup_lob.str.split('_')`.

### 3.4 `int_vw_blending_factors_with_forecast_ccy`

Picks `required_currency` from `cds_cat_class_name`:

```sql
CASE WHEN cds_cat_class_name LIKE '% UK %' THEN 'GBP'
     WHEN cds_cat_class_name LIKE '% EU %' THEN 'EUR'
     ELSE 'GBP'  END AS required_currency
```

Joins `reference.fx_rates` filtered to (USD, EUR, GBP), attaches `rate_to_gbp`.

polars: `stages.forecast.attach_currency(frame, fx_rates)`.

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

polars: `stages.combine.apply_factors_to_ylt(ylt, factors)` — **todo**.

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

polars: `stages.euws.apply_euws_factor(frame, air_events, euws_rate_factors)` — **todo**.

The special-case `HIC_HH_UK AND rnk<=100 → 1.0` is important. It only kicks
in for the top-100 UK household events; the rationale (from january) was
that euws factors aren't meaningful for the largest UK HH tail events.

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

polars: `stages.fa_gross.apply_fineart_adjustments(frame, fa_adj)` — **todo**.

### 4.4 `mts_tbl_ylt_combined_all_factors` (the physical cache)

duckdb materialises the output of 4.3 as a **BASE TABLE**. Every downstream
view reads from `..._from_cachetbl` variants to avoid recomputing. January's
readme notes this was added because recomputing the fan tree for 20+
downstream tables was too slow.

polars equivalent: `build_all_factors(...).cache()` at orchestrator level
(see `rollup/pipeline.py::run`). `.cache()` ensures the `LazyFrame` is
computed exactly once even when read by all 21 fan-out sinks.

This is the `AllFactorsCol` + `MetricCol` node in our schemas: the wide
table of dims + 6 derived loss metrics + (implicitly) the pre-joined factor
columns.

---

## 5. Long-form + aggregation for fan-out

### 5.1 `mts_vw_ylt_combined_all_factors_long_from_cachetbl`

UNPIVOT the wide cache into long format — one row per (key, metric):

```sql
UNPIVOT (value FOR metric IN (
  'original_ylt_loss', 'original_ylt_loss_uplifted', ...13 total...
))
```

polars: `frame.unpivot(on=[metric names], index=[keys], variable_name='metric',
value_name='value')`.

### 5.2 `..._aggd_for_cds_from_cachetbl`

Group to the Hisco grain (model_eventid, yearid, eventid, ccy, cds class):

```sql
SELECT base_model, model_eventid, yearid, eventid,
       required_currency AS ccy, 0 AS yoa,
       cds_cat_class_name, metric,
       SUM(value) AS value
GROUP BY base_model, model_eventid, yearid, eventid, ccy, cds_cat_class_name, metric;
```

polars: `.group_by([...]).agg(pl.sum('value'))`.

### 5.3 Fan-out to Hisco tables

Two variants:
- **AIR** (`fanout_air`): filter `base_model='vk'`, left-join `air_events`
  on `model_eventid = EventID` for `ModelEventDay = ae."Day"`.
- **RL** (`fanout_rl_nodayid` / `_withdayid`): filter `base_model='rl'`,
  `ModelEventID = eventid` (note: uses `eventid`, not `model_eventid`);
  `_withdayid` additionally joins `flood_rl22_model_events` on
  (ModelEventID, ModelYear = ModelOccurrenceYear) and computes
  `ModelEventDay = date_part('doy', ModelOccurrenceDate)`.

Then a final filter per Hisco flavour picks one metric as
`ModelGrossLoss` (the flavour → metric mapping is in
`rollup/pipeline.py::_METRIC_BY_FLAVOR`).

polars: `stages.fanout.fanout_hisco(agg, variant)` — already wired in
`pipeline.fanout_hisco`. The `ModelEventDay` computation (AIR `"Day"`, RL
from `flood_rl22_model_events`) is **todo** — currently hard-coded to 0.

---

## 6. EP curves (`mts_vw_ep_combined_all_factors*`)

Three flavours (overall / by_cds_class / by_lob / copilot), all with the
same pattern:

```
per_year(key, yearid) = sum(value)       -- for AEP
                      | max(value)       -- for OEP
rnk = row_number(per_year) order by value desc, partition by key
AAL = CASE base_model
        WHEN 'rl' THEN sum(value)/100000.0
        WHEN 'vk' THEN sum(value)/10000.0
      END
rp  = CASE base_model
        WHEN 'rl' THEN 100000/rnk
        WHEN 'vk' THEN 10000/rnk
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

## 7. Dials-up funnel (`mts_vw_ylt_dialsup__funnel`)

The "dials-up" flavour computes per-event ratios of each forecasted metric
to its local-ccy baseline, then applies those ratios back to
`original_ylt_loss`:

```
f1 = localccy_202601_euws_fagross / localccy
f2 = localccy_202607_euws_fagross / localccy
f3 = localccy_202701_euws_fagross / localccy
dialsup_{year} = f_{year} * original_ylt_loss
```

polars: `stages.dialsup.build_dialsup(agg_long)` — **todo**. Pivot long→wide,
compute ratios, pivot back.

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

polars: `tests/test_invariants.py` — **todo**.

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

| # | polars module              | calcs replaced                                                      | status |
| - | -------------------------- | ------------------------------------------------------------------- | ------ |
| 1 | `stages/staging.py`        | `int_vw_rl_ylt`, `int_vw_vk_ylt`                                    | rl done, vk todo |
| 2 | `stages/ep_summary.py`     | `vw_ep`                                                             | todo   |
| 3 | `stages/funnel.py`         | `int_vw_funnel_ylt_combined_ranked*`, `..._valid`                   | todo   |
| 4 | `stages/blending.py`       | `int_vw_blending__vendor_proportions_all_rps_pre_factors`, `..._applied` | todo   |
| 5 | `stages/forecast.py`       | `int_vw_blending_factors_with_forecast{,_ccy,_ylt_ready}`           | todo   |
| 6 | `stages/combine.py`        | `mts_vw_ylt_combined_with_blending_factors_fx_applied`              | todo   |
| 7 | `stages/euws.py`           | `..._fx_forecasted_euws_applied`                                    | todo   |
| 8 | `stages/fa_gross.py`       | `..._fx_forecasted_euws_applied_fagross`                            | todo   |
| 9 | `pipeline.build_all_factors` | cache the above as `mts_tbl_ylt_combined_all_factors` equivalent  | stub   |
|10 | `stages/fanout.py`         | `marts.*fanout_air`, `*fanout_rl_nodayid`, `*fanout_rl_withdayid`   | skeleton in `pipeline.fanout_hisco` (ModelEventDay still TODO) |
|11 | `stages/ep.py`             | `mts_vw_ep_combined_all_factors*`                                   | done (scalar version per (vendor, lob, region_peril)); by_cds_class / by_lob slices are simple group_by variants |
|12 | `stages/dialsup.py`        | `mts_vw_ylt_dialsup__funnel` + fanouts                              | todo   |
|13 | `tests/test_invariants.py` | `verify.*`                                                          | todo   |
