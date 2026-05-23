# Loading your data

Inputs belong under `data/`. Generated outputs belong under root `output/`.

## Required analyst drop locations

| Data | Location |
| --- | --- |
| Verisk YLT parquet | `data/ylt/verisk/*.parquet` |
| RiskLink YLT parquet | `data/ylt/risklink/*.parquet` |
| Verisk EP summary | `data/ep_summaries/verisk/verisk_ep_summary.long.csv` |
| RiskLink EP summary | `data/ep_summaries/risklink/rms_ep_summary.long.csv` |
| LOB lookup | `data/seeds/business/lobs.csv` |
| Peril lookup | `data/seeds/business/perils.csv` |
| Blending factors | `data/seeds/vor/blending_factors.csv` |
| FX rates | `data/seeds/vor/fx_rates.csv` |
| Forecast factors | `data/seeds/vor/forecast_factors.csv` |
| EUWS rate factors | `data/seeds/vor/euws_rate_factors.csv` |
| EUWS rank overrides | `data/seeds/adjustments/euws_rank_overrides.csv` |
| Verisk event catalogue | `data/seeds/validation/verisk_events.parquet` |
| RiskLink flood event catalogue | `data/seeds/validation/risklink_flood22_model_events.parquet` |

## EP summary long format

EP summaries consumed by the pipeline must be canonical long CSVs under
`data/ep_summaries/**/*.long.csv`. The standard files are:

- `data/ep_summaries/verisk/verisk_ep_summary.long.csv`
- `data/ep_summaries/risklink/rms_ep_summary.long.csv`

Required columns:

```text
vendor,analysis_id,modelled_lob,modelled_peril,ep_type,return_period,loss
```

Use the generator when starting from source wide CSV extracts instead of
canonical long CSVs. Put source files in `data/ep_summaries/<vendor>/`; the
scanner excludes existing `*.long.csv` outputs:

```bash
uv run rollup generate-ep-summaries
uv run rollup generate-ep-summaries --vendor verisk --csv verisk_clean.csv --yes
```

See [Creating EP summary long CSVs from wide CSVs](data-requirements.md#creating-ep-summary-long-csvs-from-wide-csvs)
for the source CSV schema, metric naming, output paths, generated columns, and
schema-file guidance.

## Seed lookup checks

Check these before validating:

- `data/seeds/business/lobs.csv` must contain every EP `modelled_lob` and every
  YLT modelled LOB. It maps to rollup LOB, class, office, currency, and CDS class
  metadata.
- `data/seeds/business/perils.csv` must contain every EP `modelled_peril` and
  every YLT modelled peril. It maps to rollup peril, region/peril labels,
  `region_peril_id`, and `selection_priority`.

## Validate the drop

```bash
uv run rollup validate
```

The default anti-join report prints real missing-input errors. Common failures:

- EP `modelled_lob` is not in `lobs.csv`.
- EP `modelled_peril` is not in `perils.csv`.
- Verisk YLT `ExposureAttribute` is not in `lobs.csv`.
- Verisk YLT `Analysis` is not in `perils.csv`.

The anti-join report should be empty. Any rows are blocking errors to fix in the
seed lookup or the input data.
