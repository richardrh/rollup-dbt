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

## Validate the drop

```bash
uv run rollup validate
```

The default anti-join report prints real missing-input errors. Common failures:

- EP `modelled_lob` is not in `lobs.csv`.
- EP `modelled_peril` is not in `perils.csv`.
- Verisk YLT `ExposureAttribute` is not in `lobs.csv`.
- Verisk YLT `Analysis` is not in `perils.csv`.
