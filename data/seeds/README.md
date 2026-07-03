# data/seeds

Seed inputs are CSV files validated against the validnator YAML contracts in this tree.

Business seeds:

- `business/lobs.csv`: maps source `modelled_lob` values to rollup LOB,
  class, office, currency, and CDS class metadata.
- `business/perils.csv`: maps source `modelled_peril` values to rollup peril,
  region/peril labels, and region-peril IDs.

VOR/adjustment seeds:

- `vor/blending_factors.csv`: blend weights by rollup peril and return-period
  bucket.
- `vor/fx_rates.csv`: currency conversion rates into GBP.
- `vor/forecast_factors.csv`: forecast-date factors by class and office.
- `vor/euws_rate_factors.csv`: Europe Windstorm event-level rate factors.
- `adjustments/euws_rank_overrides.csv`: configured overrides for selected
  zero EUWS factors on top-ranked rows.

Validation catalogues:

- `validation/verisk_events.parquet`: Verisk event/year/model mapping and event
  days.
- `validation/risklink_flood22_model_events.parquet`: RiskLink flood occurrence
  dates used to derive event-day values.

EP summaries under `data/ep_summaries/**/*.long.csv`, including
`data/ep_summaries/risklink/rms_ep_summary.long.csv` and
`data/ep_summaries/verisk/verisk_ep_summary.long.csv`, are the source/version of
truth for what gets modelled. No `selected_analyses.csv` is used. Selected scope
comes from the canonical long EP summaries plus `business/lobs.csv` and
`business/perils.csv`; YLTs under `data/ylt/verisk/*.parquet` and
`data/ylt/risklink/*.parquet` are filtered/enriched from the selected EP summary
variants.
