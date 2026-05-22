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

EP summaries must be canonical long CSVs under `data/ep_summaries/**/*.long.csv`.
The standard drop files are:

- `data/ep_summaries/verisk/verisk_ep_summary.long.csv`
- `data/ep_summaries/risklink/rms_ep_summary.long.csv`

Required columns:

```text
vendor,analysis_id,modelled_lob,modelled_peril,ep_type,return_period,loss
```

Use the generator when starting from source canonical wide CSV extracts instead
of canonical long CSVs. It scans `data/ep_summaries/<vendor>/*.csv` and excludes
existing `*.long.csv` outputs:

```bash
uv run rollup generate-ep-summaries
uv run rollup generate-ep-summaries --vendor verisk --csv verisk_clean.csv --yes
```

Wide CSVs use row 1 as the header and require `id`, `modelled_lob`, and
`modelled_peril`; `id` becomes `analysis_id`. Metric columns should be uppercase
without a `.0` suffix:

```csv
id,modelled_lob,modelled_peril,AAL_0,AEP_50,OEP_100
ANALYSIS_1,Property,US_WS,1250,1750472,2250000
```

Verisk-clean aliases `ExposureAttribute` -> `modelled_lob` and `Analysis` ->
`modelled_peril` are accepted. If present, `CatalogTypeCode` filters to trimmed
`STC` rows.

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
