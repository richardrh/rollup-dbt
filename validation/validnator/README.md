# Validnator upstream input checks

This directory contains validnator pipeline YAMLs for validating rollup inputs
before the Dataiku/rollup runtime consumes them. The runtime still keeps minimal
Polars `pl.Schema` required-column/path guards for safety; these configs are the
more detailed upstream CSV validation layer.

Run a validation with:

```bash
uv run validnator validate \
  -p validation/validnator/ep_summary_long.yml \
  -i data/ep_summaries/vendor/example.long.csv \
  -o validation-output/ep-summary
```

Seed examples:

```bash
uv run validnator validate -p validation/validnator/lobs.yml -i data/seeds/lobs.csv -o validation-output/lobs
uv run validnator validate -p validation/validnator/perils.yml -i data/seeds/perils.csv -o validation-output/perils
uv run validnator validate -p validation/validnator/fx_rates.yml -i data/seeds/fx_rates.csv -o validation-output/fx-rates
```

## YLT parquet limitation

Current validnator input loading supports CSV files only. Rollup currently reads
YLT files from `data/ylt/verisk/*.parquet` and `data/ylt/risklink/*.parquet`, so
the parquet inputs are still protected at runtime by the minimal `pl.Schema`
guards in rollup.

The `verisk_ylt_csv.yml` and `risklink_ylt_csv.yml` files are CSV-equivalent
rules for teams that validate source extracts before converting them to parquet:

```bash
uv run validnator validate -p validation/validnator/verisk_ylt_csv.yml -i exports/verisk_ylt.csv -o validation-output/verisk-ylt
uv run validnator validate -p validation/validnator/risklink_ylt_csv.yml -i exports/risklink_ylt.csv -o validation-output/risklink-ylt
```

Do not use those CSV configs as evidence that validnator has validated parquet
directly; replace or supplement them when validnator gains parquet input support.
