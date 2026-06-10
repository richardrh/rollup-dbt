# First run

Use this page for a local smoke run. Dataiku callers should use the
programmatic API described in [Programmatic API](programmatic-api.md).

## 1. Prepare inputs

Place inputs under `data/`:

```text
data/
  ylt/verisk/*.parquet
  ylt/risklink/*.parquet
  ep_summaries/**/*.long.csv
  seeds/business/lobs.csv
  seeds/business/perils.csv
  seeds/vor/blending_factors.csv
  seeds/vor/fx_rates.csv
  seeds/vor/forecast_factors.csv
  seeds/vor/euws_rate_factors.csv
  seeds/adjustments/euws_rank_overrides.csv
  seeds/validation/verisk_events.parquet
  seeds/validation/risklink_flood22_model_events.parquet
```

## 2. Run locally

```bash
uv run python -m rollup run --data-root data --output-root output --target-currency GBP
```

Faster smoke run without stage outputs or analysis:

```bash
uv run python -m rollup run --data-root data --output-root output --target-currency GBP --no-stage-outputs --no-analysis
```

Optional DuckDB export:

```bash
uv run python -m rollup run --data-root data --output-root output --target-currency GBP --duckdb
```

## 3. Check outputs

The CLI summary prints absolute paths and status for logs, marts, analysis,
stage outputs, and DuckDB. Logs default to `output/rollup.log`.

Expected default output layout is documented in [Runtime guide](runtime.md).
