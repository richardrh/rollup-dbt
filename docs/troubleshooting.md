# Troubleshooting

## Start with validation

```bash
uv run rollup validate
```

Validation checks schemas and modelled LOB/peril references. The anti-join report
shows values in EP summaries or YLT data that are missing from `lobs.csv` or
`perils.csv` (data-to-seed direction only). Seed rows without matching data do not
appear in the report and are silently ignored downstream.

## Common validation failures

- Missing, extra, or wrong-type columns versus the relevant validnator contract.
  Fix the file before running when the `Validation report` shows `valid=False`.
- EP summary `modelled_lob` missing from `data/seeds/business/lobs.csv`.
- EP summary `modelled_peril` missing from `data/seeds/business/perils.csv`.
- Verisk YLT `ExposureAttribute` missing from `lobs.csv`.
- Verisk YLT `Analysis` missing from `perils.csv`.
- Non-empty `Modelled LOB/peril anti-join report`. This is blocking: add/fix the
  value in `lobs.csv`/`perils.csv` or correct the input data.

The `YLT loss validation summary` is mainly a sanity check. Review file names,
loss sums, and scaled loss for obvious issues; it is not blocking unless an input
read failed.

## Empty or surprising outputs

Run with debug output:

```bash
uv run rollup run --debug
```

Inspect likely choke points:

```bash
duckdb -c "SELECT COUNT(*) FROM 'output/debug/stg_ep_summaries_selected.parquet';"
duckdb -c "SELECT COUNT(*) FROM 'output/debug/int_ep_blending_targets.parquet';"
duckdb -c "SELECT COUNT(*) FROM 'output/debug/int_ylt_blending_applied.parquet';"
duckdb -c "SELECT vendor, rollup_peril, COUNT(*) rows FROM 'output/debug/mts_ylt_combined_all_factors.parquet' GROUP BY 1,2 ORDER BY 1,2;"
```

## Missing FX or forecast factors

- Missing FX rows are not defaulted. Add the required currency-to-GBP rate in
  `data/seeds/vor/fx_rates.csv`.
- Missing forecast factors default to `1.0`.

## EP report missing

`ep_report.csv` is generated explicitly from existing pipeline outputs:

```bash
uv run rollup analyze
```

## SQL Server connection check failures

Check the local SQL config with:

```bash
uv run rollup sql-check --config rollup.local.toml
```

`rollup.local.toml` should stay uncommitted because it may contain credentials.
`rollup run` writes files only; load `output/marts/*.parquet` through Dataiku or
a separate SQL-loading process after the pipeline run completes.
