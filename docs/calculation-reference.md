# Calculation reference

This page explains the calculation flow for EP summaries and YLT blending.

## EP summary staging

The pipeline reads canonical long CSVs from `data/ep_summaries/**/*.long.csv`.
Each file must contain:

```text
vendor,analysis_id,modelled_lob,modelled_peril,ep_type,return_period,loss
```

During staging, EP summaries are enriched by joining `modelled_lob` to
`lobs.csv` and `modelled_peril` to `perils.csv`.

For the **main branch**, the pipeline selects one modelled peril per
`vendor + rollup_lob + rollup_peril`. The selected candidate is the one with the
lowest `selection_priority`; missing priorities are treated as `99`.

For **DIALSUP**, selection is independent of the main branch. It uses candidates
where `is_dialsup = 1` in `perils.csv`.

## Vendor EP summary join

After main-branch selection, Verisk and RiskLink losses are aggregated by:

```text
rollup_lob, rollup_peril, region_peril_id, ep_type, return_period
```

Verisk rows produce `verisk_loss`; RiskLink rows produce `risklink_loss`. The two
aggregates are full/coalesced joined so either vendor can be present in the
joined EP view.

## Blending target calculation

Blending targets are calculated only for:

- `AAL` with `return_period = 0`
- `OEP` with `return_period = 200`
- `OEP` with `return_period = 1000`

Target blend rows require both `verisk_loss` and `risklink_loss`. Blend weights
come from `blending_factors.csv` by `region_peril_id`. Europe Flood `216` is
special-cased to use subregion `216b`.

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
selected before blending.

Events are ranked within `vendor + modelled_lob + rollup_peril` by descending
loss. Return period is inferred from rank:

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
