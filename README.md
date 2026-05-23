# Rollup pipeline

Catastrophe rollup pipeline that reads analyst-supplied seed data, vendor YLT
parquets, and EP summary CSVs from `data/`, then writes mart/report outputs to
root `output/`.

Active code lives in `src/rollup/`. The CLI entrypoint is
`rollup = "rollup.cli:main"`.

## Quick start

Run commands from the repository root.

### 1. Drop analyst data under `data/`

```text
data/
  ylt/
    verisk/*.parquet
    risklink/*.parquet
  ep_summaries/
    verisk/verisk_ep_summary.long.csv
    risklink/rms_ep_summary.long.csv
  seeds/
    business/lobs.csv
    business/perils.csv
    vor/blending_factors.csv
    vor/fx_rates.csv
    vor/forecast_factors.csv
    vor/euws_rate_factors.csv
    adjustments/euws_rank_overrides.csv
    validation/verisk_events.parquet
    validation/risklink_flood22_model_events.parquet
```

Generated files go to `output/`. Do not put analyst inputs there.

EP summaries are required as canonical long CSVs under
`data/ep_summaries/**/*.long.csv`. The normal files are:

- `data/ep_summaries/verisk/verisk_ep_summary.long.csv`
- `data/ep_summaries/risklink/rms_ep_summary.long.csv`

Each EP summary row must use these columns:

```text
vendor,analysis_id,modelled_lob,modelled_peril,ep_type,return_period,loss
```

If you only have source workbook extracts, generate the canonical long files
before validating:

```bash
uv run rollup generate-ep-summaries
```

Before validation, check the business seed lookups:

- `data/seeds/business/lobs.csv` must contain every EP `modelled_lob` and every
  YLT modelled LOB. It maps modelled LOBs to rollup LOB, class, office,
  currency, and CDS class metadata.
- `data/seeds/business/perils.csv` must contain every EP `modelled_peril` and
  every YLT modelled peril. It maps modelled perils to rollup peril,
  region/peril labels, `region_peril_id`, and `selection_priority`.

### 2. Validate inputs

```bash
uv run rollup validate
```

To also write each validation table to a separate CSV file, provide a report
directory:

```bash
uv run rollup validate --report-dir output/validation
```

Console output still prints. The report directory receives:

- `validation_report.csv`
- `modelled_lob_peril_anti_join_report.csv`
- `ylt_loss_validation_summary.csv`
- `input_ylt_aal_by_lob_peril_summary.csv`

If the project is installed and your virtual environment is activated, `uv` is
optional:

```bash
rollup validate
```

Validation checks schemas and modelled LOB/peril lookups. Expected files,
columns, dtypes, and required flags are defined by the colocated `schema.yaml`
contracts under `data/seeds/`, `data/ylt/`, and `data/ep_summaries/`.
Validation includes:

- EP summary `modelled_lob` values exist in `data/seeds/business/lobs.csv`.
- EP summary `modelled_peril` values exist in `data/seeds/business/perils.csv`.
- Verisk YLT `ExposureAttribute` values exist in `lobs.csv`.
- Verisk YLT `Analysis` values exist in `perils.csv`.

Read the output in four sections:

1. `Validation report`: file-level schema, required-column, and type checks for
   seeds, YLTs, and EP summaries. `valid=False` means fix the file format before
   running the pipeline.
2. `Modelled LOB/peril anti-join report`: should be empty. Any rows are
   blocking lookup failures; add/fix the value in `lobs.csv`/`perils.csv` or fix
   the input data.
3. `YLT loss validation summary`: non-blocking sanity totals unless an input
   read failed. Check file names, loss sums, and scaled loss for obvious issues.
4. `Input YLT AAL by LOB/peril summary`: raw input YLT AAL by vendor,
   rollup/modelled LOB, and rollup/modelled peril, sorted largest-to-smallest by
   `raw_aal`. This is an analyst sanity check before business blending, FX,
   forecast, or EUWS adjustments.

### 3. Run the pipeline

```bash
uv run rollup run
```

Use debug mode when you need intermediate inspection frames:

```bash
uv run rollup run --debug
```

`rollup run` also regenerates `output/analysis/ep_report.csv` after the pipeline
outputs are written.

### 4. Optional: check SQL Server and push mart fanouts

Copy `rollup.example.toml` to `rollup.local.toml` and set the `[sql]` connection
details. `rollup.local.toml` is gitignored; do not commit credentials.

Check connectivity without running the pipeline:

```bash
uv run rollup sql-check --config rollup.local.toml
# legacy alias also works:
uv run rollup test-sql --config rollup.local.toml
```

Run the local pipeline, regenerate `output/analysis/ep_report.csv`, then push
only `output/marts/*.parquet` to SQL Server:

```bash
uv run rollup run --push-sql --config rollup.local.toml
```

SQL table names are derived from each mart parquet stem, with optional
`[sql].table_prefix`, under `[sql].schema`. Unsafe SQL identifiers are rejected.

### 5. Generate the EP report explicitly

```bash
uv run rollup analyze
```

This reads existing pipeline outputs and writes `output/analysis/ep_report.csv`.

## Outputs

Main run outputs:

```text
output/
  marts/
    HiscoAIR_YYYYMM_main.parquet
    HiscoRMS_YYYYMM_main.parquet
    HiscoAIR_YYYYMM_dialsup.parquet
    HiscoRMS_YYYYMM_dialsup.parquet
  mts_tbl_ylt_combined_all_factors.parquet
  mts_tbl_ylt_dialsup.parquet
  mts_event_validation.parquet
  debug/                         # only when --debug is used
  analysis/ep_report.csv          # after rollup analyze
```

`mts_tbl_ylt_combined_all_factors.parquet` is the row-level wide/factor-enriched
YLT mart built from `ylt_euws_override_applied`. It includes loss, rank/return
period, FX, forecast, EUWS, LOB/peril, model, and event fields.

## Inspect and validate output

Use DuckDB for quick checks:

```bash
duckdb -c "SELECT COUNT(*) FROM 'output/mts_tbl_ylt_combined_all_factors.parquet';"
duckdb -c "SELECT vendor, rollup_peril, COUNT(*) rows FROM 'output/mts_tbl_ylt_combined_all_factors.parquet' GROUP BY 1,2 ORDER BY 1,2;"
duckdb -c "SELECT * FROM 'output/mts_event_validation.parquet' LIMIT 20;"
duckdb -c "SELECT * FROM read_csv_auto('output/analysis/ep_report.csv') LIMIT 20;"
```

Debug run inspection examples:

```bash
uv run rollup run --debug

duckdb -c "SELECT * FROM 'output/debug/stg_ep_summaries_selected.parquet' LIMIT 10;"
duckdb -c "SELECT * FROM 'output/debug/int_ylt_blending_applied.parquet' LIMIT 10;"
duckdb -c "SELECT forecast_date, rollup_peril, COUNT(*) rows FROM 'output/debug/mts_ylt_combined_all_factors.parquet' GROUP BY 1,2 ORDER BY 1,2;"
```

Debug frame prefixes are `seed_*`, `stg_*`, `int_*`, and `mts_*`.

For more detail, see `docs/architecture.md` for the data-flow chart and
`docs/data-requirements.md#seed-files` for the seed file reference.

## Running documentation

Serve the Zensical docs through the application CLI:

```bash
uv run rollup docs
uv run rollup docs --host 127.0.0.1 --port 8000
```

The command starts/serves the local docs and prints a URL. With an activated
environment, use `rollup docs` without `uv`.

Direct Zensical usage is also possible if needed:

```bash
uv run zensical serve --config-file zensical.toml --dev-addr 127.0.0.1:8000
```

## Developer guide: add a pipeline step

1. Add or modify a pure transformation function in `src/rollup/pipeline.py`.
   Prefer `LazyFrame` and keep file IO at the edges.
2. Put new shared column names in `src/rollup/columns.py` enums instead of
   scattering string literals.
3. If a step adds or changes an input, output, stage, or mart contract, update
   the appropriate colocated `schema.yaml` and validation tests.
4. Wire the function into the right phase inside `run()`: validation, staging,
   intermediate, or marts.
5. Add the result to the relevant stage dictionary: `seed_frames`,
   `staging_frames`, `intermediate_frames`, or `mart_frames`. This is what makes
   `--debug` write the frame with the correct prefix.
6. If the frame is a final/wide/mart output, update `write_mart_outputs` or the
   appropriate writer so it lands under `output/`.
7. If the frame feeds `rollup analyze`, update `src/rollup/analysis.py`.
8. Add tests under `tests/` with `tmp_path` synthetic data. Do not mutate
   production `data/`.
9. Update README/docs/examples.
10. Run checks:

```bash
uv run pytest -q
uv run rollup validate
uv run rollup run --debug
uv run rollup analyze
```
