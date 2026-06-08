# data/seeds

Seed inputs are CSV files described by `data/seeds/schema.yaml`. Upstream/dev
CSV validation uses validnator configs colocated with each seed section.
Validnator validates one CSV input at a time, so sections containing multiple
schemas use multiple clearly named `validnator-*.yml` files rather than one YAML
that would require unrelated columns in a single file.

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

There is no validnator config under `validation/` because these catalogue inputs
are parquet files and validnator currently validates CSV files only.

Validnator examples:

```bash
uv run validnator validate -p data/seeds/business/validnator-lobs.yml -i data/seeds/business/lobs.csv -o validation-output/lobs
uv run validnator validate -p data/seeds/business/validnator-perils.yml -i data/seeds/business/perils.csv -o validation-output/perils
uv run validnator validate -p data/seeds/vor/validnator-fx-rates.yml -i data/seeds/vor/fx_rates.csv -o validation-output/fx-rates
uv run validnator validate -p data/seeds/adjustments/validnator.yml -i data/seeds/adjustments/euws_rank_overrides.csv -o validation-output/euws-rank-overrides
```

EP summaries under `data/ep_summaries/**/*.long.csv`, including
`data/ep_summaries/risklink/rms_ep_summary.long.csv` and
`data/ep_summaries/verisk/verisk_ep_summary.long.csv`, are the source/version of
truth for what gets modelled. No `selected_analyses.csv` is used. Selected scope
comes from the canonical long EP summaries plus `business/lobs.csv` and
`business/perils.csv`; YLTs under `data/ylt/verisk/*.parquet` and
`data/ylt/risklink/*.parquet` are filtered/enriched from the selected EP summary
variants.
