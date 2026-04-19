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
# 1. staging — raw YLTs → NormalizedYlt union (carries office + lob_class)
rl_norm = normalize_risklink_ylt(...)
vk_norm = normalize_verisk_ylt(...)
ylt = pl.concat([rl_norm, vk_norm])

# 2. observation — orphan count for downstream alerting
count_event_id_orphans(ylt, seeds.air_events, vendor_filter="verisk")

# 3. factors — one function per factor, each ~15 LOC in stages/factors.py
ylt = attach_currency        (ylt, seeds.fx_rates)
ylt = attach_forecast_factors(ylt, seeds.forecast_factors, tags)
ylt = attach_rank            (ylt)
ylt = attach_euws            (ylt, seeds.euws_rate_factors, seeds.euws_rank_overrides)
ylt = attach_fagross         (ylt, seeds.fineart_adjustments)
ylt = attach_uplift          (ylt, seeds.blending_factors, seeds.dim_region_perils)

# 4. metrics — materialise the cumulative chain as columns
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
  `config.FLOOD_PERILS` to force `base_model='risklink'` for EU/UK flood).

That's it. Adding a new factor is adding one function and one call site.

## What's dynamic vs hardcoded

| | dynamic? | what it takes to add one |
|---|---|---|
| Forecast **dates** | ✅ fully | Add a row to `forecast_factors.csv`. Runtime reads the new date and emits `f_{yyyymm}` + three new metric columns + new Hisco variants. Zero code change. |
| A new **factor with per-tag multiplication** (year-varying) | ⚠ half | `attach_X()` + extend the metrics loop to chain it per tag. One new function + one edit. |
| A new **flat factor** (year-invariant) | ⚠ half | `attach_X()` + call it in `build_all_factors` + one multiplication in `_compute_metrics`. |
| The **chain order** | ❌ code | Intentional — order is semantic, not data. |

## The 5-step recipe

Adding a new factor — call it `broker_commission_factor` for concreteness:

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
   - `polars/rollup/seeds.py` — add `SeedSpec` entry + field on `Seeds` dataclass.

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
   late in the chain (after all technical factors, before the final audit):
   ```python
   ylt = attach_fagross(ylt, seeds.fineart_adjustments)
   ylt = attach_broker_commission(ylt, seeds.broker_commissions)  # new line
   ylt = attach_uplift(ylt)
   ```

**5. Pick a column suffix + chain the multiplication**

   Convention: one-word suffix appended to the cumulative metric name. For
   broker commission use `_brokercomm`. In `_compute_metrics`:
   ```python
   for tag in tags:
       ...
       localccy_fa_bc = f"loss_uplifted_capped_localccy_{tag}_euws_fagross_brokercomm"
       ylt = ylt.with_columns(
           (pl.col(localccy_fa) * pl.col(AF.BROKER_COMMISSION_FACTOR)).alias(localccy_fa_bc),
       )
   ```
   Update `audit_wide`'s column ordering so the new factor sits next to its
   metric (next to `fa_gross_*` in this case). Update `VariantSpec.loss_metric`
   if this is the new canonical "main" metric.

That's the whole recipe. Every factor is the same five steps.

## What NOT to do

- **Don't add `if vendor == 'verisk'` branches inside factor functions.** If
  a factor only applies to one vendor, filter at the seed level (rows for
  one vendor only, other vendor rows get `fill_null(1.0)` pass-through). The
  one exception: `attach_uplift` consults `config.FLOOD_PERILS` to force
  the base model — promoted to a config constant precisely so it isn't a
  hidden branch inside the function body.
- **Don't change column-naming order.** `loss_uplifted_capped_localccy_{tag}_euws_fagross`
  reads strictly in chain order. Breaking this breaks the audit.
- **Don't drop `fill_null(1.0)`** in the multiplicative factors (forecast,
  euws, fa_gross). Missing reference rows pass through as 1.0 by design.
  *Exception*: FX is not a multiplicative factor — a missing rate is a real
  problem, so `attach_currency` raises rather than fills.
- **Don't split one factor into multiple stages.** If your factor has a
  special case (like euws's rank-threshold override), keep the case inside
  the same `attach_*` function — that's where the reviewer expects to find it.

## See also

- [`../rollup/stages/factors.py`](../rollup/stages/factors.py) — the
  functions themselves, with the 5-step recipe repeated in the module
  header for code readers.
- [`calculations.md`](calculations.md) — per-factor mapping to january's
  duckdb views, with the original SQL quoted.
