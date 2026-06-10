# Calculation reference

This page documents the current calculation contracts behind the runtime guide.

## Stage order

The pipeline runs:

1. `load_sources`
2. `normalize_ylt`
3. `stage_ep_summaries`
4. `build_enriched_ylt`
5. `apply_blending`
6. `apply_fx`
7. `apply_forecast`
8. `apply_euws`
9. `build_metric_long`
10. `build_dialsup`
11. `write_marts`

## EP summary staging

EP summaries are read from `data/ep_summaries/**/*.long.csv` with columns:

```text
vendor,analysis_id,modelled_lob,modelled_peril,ep_type,return_period,loss
```

`stage_ep_summaries` enriches them by joining:

- `modelled_lob` to `seeds/business/lobs.csv`
- `modelled_peril` to `seeds/business/perils.csv`

For the main branch, it selects the lowest `selection_priority` per:

```text
vendor, rollup_lob, rollup_peril
```

It also preserves `is_dialsup` at rollup peril level. This flag drives the
separate DIALSUP branch and does not change main-branch selection.

## YLT enrichment

`normalize_ylt` converts vendor YLTs into a canonical shape. Verisk carries
modelled dimensions in the YLT. RiskLink raw YLT is keyed by analysis id, so
modelled LOB/peril should come from the EP summary enrichment.

Known follow-up: `Pen` and `Cherish` RiskLink rows currently have null
`modelled_lob` and `modelled_peril` even though EP summaries contain `MGA_Pen`
and `MGA_Cherish`; this is likely due to dropping those fields before the
RiskLink `analysis_id` join in `build_enriched_ylt`.

## EP-derived blending

Blending uses the restored old-master method:

- target points: `AAL`, `OEP 200`, and `OEP 1000`
- blending weights from `seeds/vor/blending_factors.csv`
- `target_loss = verisk_loss * AIRBlend + risklink_loss * RMSBlend`
- base model is RiskLink for Europe/UK flood and Verisk otherwise
- `base_model_loss` comes from the chosen base model
- `uplift_factor_on_base_model = target_loss / base_model_loss`
- uplift factors are clipped to `0.1..10`
- YLT rows join targets by rank-derived return-period bucket

Rank-derived buckets:

```text
RiskLink RP = 100000 / rank
Verisk RP   = 10000 / rank

RP < 200   -> 0
RP < 1000  -> 200
RP >= 1000 -> 1000
```

Missing required blending weights are a follow-up candidate for an explicit
error; today they can cause downstream row loss or join failures depending on the
case.

## FX

The target currency is explicit and defaults to `GBP`. A non-empty FX seed must
include the requested `target_currency`; missing non-target rates fail rather
than silently defaulting. Metric names include the target currency tag, for
example `loss_blended_fx_gbp` or `loss_blended_fx_usd`.

## Forecast

Forecast expands each row across all forecast dates from
`seeds/vor/forecast_factors.csv`. The join is by class, office, and forecast
date. Missing class/office/date factors default to `1.0`.

## EUWS and overrides

EUWS factors are event based. The runtime uses Verisk event catalogues,
`euws_rate_factors.csv`, and `euws_rank_overrides.csv` to restore the old-master
EUWS adjustment and rank override behavior.

## DIALSUP

DIALSUP uses original YLT loss × FX × forecast. It does **not** use blended loss
or EUWS-adjusted loss. Rows are selected by `is_dialsup == 1` from the enriched
rollup peril mapping.

DIALSUP writes `mts_tbl_ylt_dialsup.parquet`. DIALSUP fanout files are not
emitted separately today.

## Metrics

Combined long metrics:

- `loss_original_ylt`
- `loss_blended`
- `loss_blended_fx_gbp`
- `loss_blended_fx_gbp_forecast`
- `loss_blended_fx_gbp_forecast_euws_override`

DIALSUP metric:

- `loss_dialsup_fx_gbp_forecast`

When target currency changes, the metric tag changes too, e.g.
`_fx_usd_...`.

## Wide mart

`mts_tbl_ylt_combined_all_factors_wide.parquet` is a pivot of the combined
all-factors long mart only. It has no `metric`, `forecast_date`, or `loss`
columns. Value columns are named `{metric}_{forecast_date_without_hyphens}`.

DIALSUP stays in `mts_tbl_ylt_dialsup.parquet` and is not included in combined
wide.

## Reference smoke values

Against real `./data`, combined sums are approximately:

- `loss_original_ylt`: `595,127,587,394.46`
- `loss_blended`: `579,116,007,376.25`
- `loss_blended_fx_gbp`: `577,222,053,036.84`
- `loss_blended_fx_gbp_forecast`: `566,796,627,725.94`
- `loss_blended_fx_gbp_forecast_euws_override`: `566,250,261,028.68`

EP AAL smoke values:

- main/EUWS: `11,175,803.275055`
- DIALSUP: `12,772,490.495922`

These are smoke references, not hard guarantees. Current calculations match the
Jun6 master within float noise while using the modernized output shape and metric
names.
