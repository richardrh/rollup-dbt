# Data requirements

Inputs live under `data/`. Generated files live under root `output/`.
Schema contracts for required files, columns, and dtypes live in colocated
`schema.yaml` files; see [Schema contracts](schema-contracts.md).

## Required analyst inputs

| Data | Location |
| --- | --- |
| Verisk YLT parquet | `data/ylt/verisk/*.parquet` |
| RiskLink YLT parquet | `data/ylt/risklink/*.parquet` |
| Verisk EP summary | `data/ep_summaries/verisk/verisk_ep_summary.long.csv` |
| RiskLink EP summary | `data/ep_summaries/risklink/rms_ep_summary.long.csv` |
| Seeds | `data/seeds/**` |

EP summaries must be canonical long CSVs with:

```text
vendor,analysis_id,modelled_lob,modelled_peril,ep_type,return_period,loss
```

If source data arrives as workbooks, generate the long files first:

```bash
uv run rollup generate-ep-summaries
```

## Seed files

Seed schema contracts are defined in `data/seeds/schema.yaml`; see
[Schema contracts](schema-contracts.md) for how these YAML files anchor required
columns and validation. `uv run rollup validate` reports schema issues and runs
anti-join validation for LOB/peril coverage. The anti-join report should be
empty before running the pipeline.

### `data/seeds/business/lobs.csv`

Business LOB lookup. It maps vendor/modelled LOB values in `modelled_lob` to
`rollup_lob`, `lob_type`, CDS class, office, class, and currency.

The pipeline uses it to enrich EP summaries and YLT rows. Downstream class,
office, and currency-driven logic depends on this file, including forecast and FX
application.

### `data/seeds/business/perils.csv`

Peril lookup from GC/vendor `modelled_peril` values to rollup peril labels,
region/peril labels, and `region_peril_id`.

`selection_priority` chooses the preferred modelled peril variant when multiple
modelled perils map to the same vendor, `rollup_lob`, and `rollup_peril`. Lower
numbers win. Missing priorities are filled as `99` during EP staging; schema text
uses `99` as the normal fallback priority, and some calling contexts treat the
fallback/default as `99`/`100`.

### `data/seeds/vor/blending_factors.csv`

VOR blend weights by `RegionPerilID` and `SubRegionPerilID`. The EP blend target
step uses the AIR and RMS weights to create blended loss targets from Verisk and
RiskLink EP summaries.

Europe Flood `RegionPerilID` `216` is special-cased to use
`SubRegionPerilID` `216b`.

### `data/seeds/vor/fx_rates.csv`

FX lookup from source currency to target currency. The pipeline filters this file
to GBP targets and joins by source currency to produce GBP losses.

Missing FX rows are not defaulted: the join is inner, so rows without a GBP FX
rate are dropped rather than carried forward with `1.0`.

### `data/seeds/vor/forecast_factors.csv`

Forecast multipliers by class, office, and forecast date. The pipeline expands
YLT rows across available forecast dates and applies the matching factor.

Missing class/office/date factors default to `1.0`.

### `data/seeds/vor/euws_rate_factors.csv`

Event-level Europe Windstorm factors by model event and occurrence year. These
are applied only to `Europe_WS` rows after joining YLT events to the Verisk event
catalogue. Non-Europe Windstorm rows use factor `1.0`.

### `data/seeds/adjustments/euws_rank_overrides.csv`

Override file for selected zero EUWS factors. It applies by `rollup_lob` to
top-ranked rows where the raw EUWS factor is zero and the row rank is within the
configured maximum rank.

### `data/seeds/validation/verisk_events.parquet`

Validation/event catalogue mapping Verisk event, year, and model code to model
event id and event day. It supports EUWS factor joins, DIALSUP, and event-date
fields in outputs.

### `data/seeds/validation/risklink_flood22_model_events.parquet`

RiskLink flood event occurrence-date catalogue. The mart fanout uses it to derive
event day fields for RiskLink flood rows.

## Validation checklist

- Every EP `modelled_lob` and YLT modelled LOB must exist in `lobs.csv`.
- Every EP `modelled_peril` and YLT modelled peril must exist in `perils.csv`.
- Inputs must match their colocated `schema.yaml` contracts for required files,
  columns, and types.
- Run `uv run rollup validate`; treat any LOB/peril anti-join rows as blocking
  input or lookup errors.
