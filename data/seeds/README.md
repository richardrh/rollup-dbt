# data/seeds

Seed inputs are user-owned data for pipeline. Their colocated manifest is
`data/seeds/schema.yaml`.

Pipeline expects business seeds under `data/seeds/business/`:

- `lobs.csv`
- `perils.csv`
- `analyses.csv`
- `selected_analyses.csv` — required operator scope file; header-only is valid before an operator selects analyses.

Pipeline expects VOR seeds under `data/seeds/vor/`:

- `blending_weights.csv`
- `forecast_factors.csv`
- `fx_rates.csv`
- `euws_rate_factors.csv`
