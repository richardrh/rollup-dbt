# Factor chain

The mental model for understanding how a YLT loss becomes a Hisco number,
and how to extend the pipeline with new factors.

## The chain

Every YLT loss goes through a sequence of multiplicative factors. The order
matters (FX before forecast, euws before fa_gross) because each factor's
meaning depends on what came before.

```
loss_raw
  × uplift_factor           → loss_uplifted
  × (clip 0.1..10)          → loss_uplifted_capped
  ÷ rate_to_gbp             → loss_uplifted_capped_localccy
  × f_{tag}                 → loss_uplifted_capped_localccy_{tag}              (× N tags)
  × euws_factor             → loss_uplifted_capped_localccy_{tag}_euws         (× N tags)
  × fa_gross_aal_factor     → loss_uplifted_capped_localccy_{tag}_euws_fagross (× N tags)
```

Six factors. Three year-invariant (uplift, cap, FX) + three year-varying
(forecast, euws applied to the forecasted column, fa_gross applied to the
euws column). Per forecast tag in the seed you get three year-tagged
columns. With N tags that's `3 + 3×N` metric columns.

Plus one sensitivity column per tag: `dialsup_{tag}`.

## Column-name convention: the chain is visible

Every column's name says exactly which factors have been applied. If you
see `loss_uplifted_capped_localccy_202601_euws`, you know it went through
uplift → cap → FX → 202601 forecast → euws (but not fa_gross).

This is the project's **single most important invariant**. The audit wide
parquet's column ordering is designed so that every factor column sits
immediately before the metric it produced, meaning a human can read one row
left-to-right and verify the arithmetic step by step.

## Composition

`pipeline.build_all_factors` is the composition. It reads top-down:

```python
# 1. staging — raw YLTs → NormalizedYlt union (carries office, lob_class,
#    peril_name, region, peril_family)
rl_norm = normalize_risklink_ylt(load_raw_risklink_ylt(...),
                                  seeds.analyses, seeds.perils, seeds.lobs)
vk_norm = normalize_verisk_ylt  (load_raw_verisk_ylt(...),
                                  seeds.analyses, seeds.perils, seeds.lobs)
ylt = pl.concat([rl_norm, vk_norm])

# 2. observation — orphan count for downstream alerting
count_event_id_orphans(ylt, seeds.air_events, vendor_filter=VendorName.VERISK)

# 3. scope filter — drop rows not officially in the rollup
ylt = apply_rollup_scope(ylt, seeds.rollup_scope)

# 4. factors — one function per factor, each ~15 LOC in stages/factors.py
ylt = attach_currency        (ylt, seeds.fx_rates)
ylt = attach_forecast_factors(ylt, seeds.forecast_factors, tags)
ylt = attach_rank            (ylt)                                      # must precede attach_euws
ylt = attach_euws            (ylt, seeds.euws_rate_factors, seeds.euws_rank_overrides)
ylt = attach_fagross         (ylt, seeds.fineart_adjustments)
ylt = attach_uplift          (ylt, seeds.blending_weights, n_sim=n_sim)

# 5. metrics — walk chain.CHAIN; multiply each stage's factor into the prev
ylt = _compute_metrics(ylt, tags)
ylt = _compute_dialsup(ylt, tags)
```

Each `attach_*` is a pure function in `rollup/stages/factors.py`:

- Read one or two seed columns.
- Left-join the YLT on the natural keys.
- `fill_null(1.0)` — missing reference data defaults to pass-through, *except*
  in `attach_currency` which raises `MissingFxRateError` because silently
  dropping FX would inflate losses.
- Optionally a special-case (`attach_euws` applies rank-threshold overrides
  from `euws_rank_overrides.csv`; `attach_uplift` consults
  `config.FLOOD_FAMILY` to force `base_model='risklink'` for any peril
  whose `peril_family == "FL"`).

That's it. Adding a new factor is adding one function and one call site.

## What's dynamic vs hardcoded

| | dynamic? | what it takes to add one |
|---|---|---|
| Forecast **dates** | ✅ fully | Add a row to `forecast_factors.csv`. Runtime reads the new date and emits `f_{yyyymm}` + cumulative metric columns + new Hisco variants. Zero code change. |
| A new **per-tag factor** (year-varying) | ⚠ half | `attach_X()` + ONE entry in `chain.CHAIN`. The metrics walker / audit layout / variant-spec all pick it up automatically. |
| A new **flat factor** (year-invariant) — applied OUTSIDE the year-tagged chain | ⚠ half | `attach_X()` + extend the year-invariant block in `_compute_metrics` (the uplift → cap → fx prelude). |
| The **chain order** | ❌ data | Order is the insertion order of `chain.CHAIN`. Reorder the dict, the column suffixes + audit layout follow. |

## The 5-step recipe

Adding a new factor — call it `broker_commission_factor` for concreteness.
This walks through the year-tagged case (the most common); year-invariant
factors skip step 5 and edit `_compute_metrics`'s prelude instead.

**1. Seed file + schema**

   `polars/seeds/broker_commissions.csv`:
   ```
   lob_id,commission_factor
   1,0.85
   2,0.90
   ```
   Register it:
   - `polars/rollup/schemas/columns.py` — new `RefBrokerCommissionsCol` StrEnum.
   - `polars/rollup/schemas/frames.py` — new `REF_BROKER_COMMISSIONS` schema.
   - `polars/rollup/seeds.py` — add `SeedSpec` entry + field on `Seeds` dataclass
     (and add to `REQUIRED_SEEDS` if a real run can't proceed without it).

**2. AllFactorsCol member**

   `polars/rollup/schemas/columns.py` — add one line:
   ```python
   BROKER_COMMISSION_FACTOR = "broker_commission_factor"
   ```
   Add the corresponding entry in `frames.py::ALL_FACTORS`.

**3. Write `attach_broker_commission`**

   In `polars/rollup/stages/factors.py`, mirror the pattern of
   `attach_fagross`:
   ```python
   def attach_broker_commission(ylt, broker_commissions):
       bc = broker_commissions.select(
           pl.col(BC.LOB_ID),
           pl.col(BC.COMMISSION_FACTOR).alias(AF.BROKER_COMMISSION_FACTOR),
       )
       out = (
           ylt.join(bc, on=Y.LOB_ID, how="left")
              .with_columns(pl.col(AF.BROKER_COMMISSION_FACTOR).fill_null(1.0).cast(pl.Float64))
       )
       log.info("broker_commission: factor attached (1.0 for unmapped LOBs)")
       return out
   ```

**4. Call it from `build_all_factors`**

   In `pipeline.py`, pick the right chain position. Broker commission feels
   late in the chain (after all technical factors):
   ```python
   .pipe(attach_fagross,            seeds.fineart_adjustments)
   .pipe(attach_broker_commission,  seeds.broker_commissions)   # new line
   .pipe(attach_uplift,             seeds.blending_weights, n_sim=n_sim)
   ```

**5. Add ONE entry to `chain.CHAIN` — the only edit that affects the chain**

   In `rollup/chain.py`:
   ```python
   CHAIN: dict[str, ChainStage] = {
       "forecast":  {...},
       "euws":      {...},
       "fagross":   {...},
       "brokercomm": {                                    # ← new entry
           "suffix":     "_brokercomm",
           "factor_col": AF.BROKER_COMMISSION_FACTOR,
           "is_per_tag": False,
           "ancillary_before": (),
           "ancillary_after":  (),
       },
   }
   ```

   That's it. `_compute_metrics` walks `CHAIN` and multiplies the new
   factor into the prev cumulative column to produce
   `loss_uplifted_capped_localccy_{tag}_euws_fagross_brokercomm`.
   `_metric_cols_for`, `audit_wide`, and `VariantSpec.loss_metric` (which
   reads `chain.main_loss_col(tag)` = the LAST chain entry's column) all
   pick up the new factor automatically. **No edits in `pipeline.py`.**

## What NOT to do

- **Don't add `if vendor == 'verisk'` branches inside factor functions.** If
  a factor only applies to one vendor, filter at the seed level (rows for
  one vendor only, other vendor rows get `fill_null(1.0)` pass-through). The
  one exception: `attach_uplift` consults `config.FLOOD_FAMILY` to force
  the base model when `peril_family == "FL"` — promoted to a config
  constant precisely so it isn't a hidden branch inside the function body.
- **Don't hand-build the column-name f-string anywhere.** `chain.col_after`,
  `chain.main_loss_col`, `chain.dialsup_col`, `chain.forecast_factor_col`
  are the only sanctioned builders. New f-string sites = future drift bugs.
- **Don't change the chain naming convention** (`{base}_{tag}{suffix1}{suffix2}...`).
  The audit reads strictly left-to-right in chain order; the convention is
  the contract.
- **Don't drop `fill_null(1.0)`** in the multiplicative factors (forecast,
  euws, fa_gross). Missing reference rows pass through as 1.0 by design.
  *Exception*: FX is not a multiplicative factor — a missing rate is a real
  problem, so `attach_currency` raises `MissingFxRateError` rather than fills.
- **Don't split one factor into multiple stages.** If your factor has a
  special case (like euws's rank-threshold override), keep the case inside
  the same `attach_*` function — that's where the reviewer expects to find it.

## See also

- [`../polars/rollup/stages/factors.py`](../polars/rollup/stages/factors.py) — the
  functions themselves, with the 5-step recipe repeated in the module
  header for code readers.
- [`../polars/rollup/chain.py`](../polars/rollup/chain.py) — the year-tagged
  `CHAIN` registry the metrics walker reads.
- [`calculations.md`](calculations.md) — per-factor mapping to january's
  duckdb views, with the original SQL quoted.
