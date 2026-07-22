# Calculation reference

This page explains the calculation flow for EP summaries and YLT blending.

## EP summary enrichment

The pipeline reads canonical long CSVs from `data/ep_summaries/**/*.long.csv`.
Each file must contain:

```text
vendor,analysis_id,modelled_lob,modelled_peril,ep_type,return_period,loss
```

There is no EP pass-through stage. Canonical long source summaries feed
`int_ep_summaries_enriched` directly, where `modelled_lob` joins to `lobs.csv`
and `modelled_peril` joins to `perils.csv`.

For the **main branch**, the pipeline selects one modelled peril per
`vendor + rollup_lob + rollup_peril`. The selected candidate is the one with the
lowest `selection_priority`; missing priorities are treated as `99`.

For **DIALSUP**, selection is independent of the main branch. It uses candidates
where `is_dialsup = 1` in `perils.csv`.

## Vendor EP summary join

After main-branch selection, Verisk and RiskLink losses are aggregated by:

```text
rollup_lob, rollup_peril, region_peril_id, blend_subregion_peril_id,
base_model, ep_type, return_period
```

Verisk rows produce `verisk_loss`; RiskLink rows produce `risklink_loss`. The two
aggregates are full/coalesced joined so either vendor can be present in the
joined EP view. This is distributional alignment of EP points at shared
business dimensions, not event-level alignment.

## Blending target calculation

Blending targets are calculated only for:

- `AAL` with `return_period = 0`
- `OEP` with `return_period = 200`
- `OEP` with `return_period = 1000`

When both vendor losses are present, their weighted contributions form the
target. Blend weights come from `blending_factors.csv` by `region_peril_id`.
Europe Flood `216` is special-cased to use subregion `216b`. If one vendor loss
is absent, the target remains the configured base-model loss.

```text
target_loss = (verisk_loss * AIRBlend) + (risklink_loss * RMSBlend)
```

The base model is `risklink` for `Europe_FL` and `UK_FL`, and `verisk` otherwise.

```text
base_model_loss = risklink_loss when base_model = risklink, else verisk_loss
uplift_factor_on_base_model = target_loss / base_model_loss
```

The uplift factor is clipped to `0.1`–`10.0`.

## Applying blending to YLT rows

YLT rows are enriched from the selected EP summary mapping. Base-model rows are
selected before blending: the main stream keeps `vendor == base_model`. It does
not pair RiskLink and Verisk YLT events, rank them together, or exchange their
event IDs.

Selected events are ranked within `vendor + modelled_lob + rollup_peril` by
descending loss. Return period is inferred from rank:

```text
RiskLink RP = 100,000 / rank
Verisk RP   = 10,000 / rank
```

RP bucket assignment:

```text
RP < 200      -> 0
RP < 1000     -> 200
RP >= 1000    -> 1000
```

Each YLT row is inner-joined to the blending target by:

```text
rollup_lob, rollup_peril, region_peril_id, rp_bucket, base_model
```

Then:

```text
loss = original_loss * uplift_factor_on_base_model
```

For `Europe_WS`, `perils.csv` configures Verisk as base. A RiskLink EP loss can
contribute to the target and uplift, but the resulting YLT row remains the
selected Verisk event. Its `(event_id, year_id, model_code)` joins to the
Verisk catalogue for `model_event_id` and `event_day`; EUWS factors join by
`model_event_id`. RiskLink rows have no Verisk `model_code`, are never given
Verisk catalogue IDs, and use the RiskLink occurrence catalogue/fanout path
when RiskLink is base. Unmatched Verisk-catalogue or EUWS-factor joins stay
null and use an EUWS factor of `1.0`.

## Output flow

The main branch continues after blending through FX conversion, forecast factor
expansion, EUWS factors, and EUWS rank overrides.

DIALSUP uses the independent `is_dialsup = 1` selection and applies base-model
loss, FX, and forecast factors. It does not apply blending or EUWS.

## Common surprises and row-count changes

- Inner joins can drop rows when mappings, FX rates, or blending targets are
  absent. Validation aims to catch high-risk missing inputs, but not every
  downstream row-count change is a validation failure.
- DIALSUP can have different row counts from main because it may select a
  different modelled peril.
- Wide output can be sparse when main and DIALSUP use different source perils.
  The pivot keeps lineage-preserving dimensions, so non-matching source
  dimensions do not collapse into the same row.

## See also

- [EP summaries](ep-summaries.md)
- [Architecture](architecture.md)
- Validnator YAML contracts under `data/`
