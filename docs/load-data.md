# Loading your data

Inputs belong under `data/`. Generated outputs belong under root `output/`.

## Step 1. Put files in the right place

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

YLT loading is folder-based by vendor. Put one or more parquet files directly in
the active vendor folder:

- Verisk: `data/ylt/verisk/*.parquet`
- RiskLink: `data/ylt/risklink/*.parquet`

Every direct child file ending `.parquet` in those folders is validated and
loaded. There is no required YLT filename convention beyond the `.parquet`
extension and correct vendor folder, but use clear names so operators can trace
validation messages back to source extracts. Do not leave inactive, draft, or test
parquet files in these active folders because they will be included. Parquet files
inside subdirectories are ignored by the current glob.

## Step 2. Convert EP summary CSVs if needed

The pipeline needs these `.long.csv` files:

- `data/ep_summaries/verisk/verisk_ep_summary.long.csv`
- `data/ep_summaries/risklink/rms_ep_summary.long.csv`

Required columns:

```text
vendor,analysis_id,modelled_lob,modelled_peril,ep_type,return_period,loss
```

If you have a vendor/source CSV instead:

1. Put it in `data/ep_summaries/<vendor>/`.
2. Run the interactive command:

```bash
uv run rollup generate-ep-summaries
```

Or run without prompts:

```bash
uv run rollup generate-ep-summaries --vendor verisk --csv verisk_clean.csv --yes
```

3. Check the generated `.long.csv` file listed above.
4. Run validation in Step 4.

See [Creating EP summary long CSVs from wide CSVs](data-requirements.md#creating-ep-summary-long-csvs-from-wide-csvs)
for the detailed source and output tables.

## Step 3. Check seed lookups

Raw YLT parquet files can be direct vendor extracts with harmless extra columns.
Minimum required YLT columns are:

- Verisk: `Analysis`, `ExposureAttribute`, `CatalogTypeCode`, `EventID`,
  `ModelCode`, `YearID`, `GroundUpLoss`. Do not add a row-level `filename`
  column just for validation; file names come from parquet paths.
- RiskLink: `anlsid`, `yearid`, `eventid`, `loss`. `p_value`, `meanloss`,
  `stddev`, and `expvalue` are optional if present in the export.

RiskLink YLT `anlsid` values must match RiskLink EP summary `analysis_id` values.

Check these before validating:

- `data/seeds/business/lobs.csv` must contain every EP `modelled_lob` and every
  YLT modelled LOB. It maps to rollup LOB, class, office, currency, and CDS class
  metadata.
- `data/seeds/business/perils.csv` must contain every EP `modelled_peril` and
  every YLT modelled peril. It maps to rollup peril, region/peril labels,
  `region_peril_id`, main-pipeline `selection_priority`, and DIALSUP-only
  `is_dialsup`. Use `is_dialsup = 1` for active base/least-adjusted DIALSUP
  candidates; adjusted alternatives should generally be `0`.

## Step 4. Validate the drop

```bash
uv run rollup validate
```

Common failures:

- EP `modelled_lob` is not in `lobs.csv`.
- EP `modelled_peril` is not in `perils.csv`.
- Verisk YLT `ExposureAttribute` is not in `lobs.csv`.
- Verisk YLT `Analysis` is not in `perils.csv`.
- RiskLink YLT `anlsid` is not in the RiskLink EP summary `analysis_id` values.

The anti-join report should be empty. Fix any rows before running the pipeline.
