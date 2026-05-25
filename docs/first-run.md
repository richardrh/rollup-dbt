# Quickstart

Run from the repository root. If you are on a fresh Windows machine, first use
the [Windows install guide](windows-install.md) to install `uv` and build the
local environment.

## 1. Drop the data

Put analyst inputs under `data/`:

```text
data/ylt/verisk/*.parquet
data/ylt/risklink/*.parquet
data/ep_summaries/verisk/verisk_ep_summary.long.csv
data/ep_summaries/risklink/rms_ep_summary.long.csv
data/seeds/**
```

Generated outputs land in root `output/`; do not put analyst inputs there.

YLT files must be Parquet. If a YLT extract arrives as CSV, convert it first with
the [DuckDB utility command](utilities.md#convert-a-ylt-csv-extract-to-parquet-with-duckdb).

EP summaries must be canonical long CSVs under `data/ep_summaries/**/*.long.csv`.
The normal files are `data/ep_summaries/verisk/verisk_ep_summary.long.csv` and
`data/ep_summaries/risklink/rms_ep_summary.long.csv`.

Required EP summary columns:

```text
vendor,analysis_id,modelled_lob,modelled_peril,ep_type,return_period,loss
```

If you have source XLSX/workbook data instead of canonical long CSVs, generate
the long files before validating:

```bash
uv run rollup generate-ep-summaries
```

## 2. Check seed lookups

Before validation, check these files especially:

- `data/seeds/business/lobs.csv`: must contain every EP/YLT modelled LOB; maps
  to rollup LOB, class, office, currency, and CDS class metadata.
- `data/seeds/business/perils.csv`: must contain every EP/YLT modelled peril;
  maps to rollup peril, region/peril labels, `region_peril_id`, and
  `selection_priority`.

## 3. Validate

```bash
uv run rollup validate
```

To keep CSV evidence while preserving the same console output, write validation
reports to a directory:

```bash
uv run rollup validate --report-dir output/validation
```

This creates `validation_report.csv`,
`modelled_lob_peril_anti_join_report.csv`,
`ylt_loss_validation_summary.csv`, and
`input_ylt_aal_by_lob_peril_summary.csv` under `output/validation/`.

Validation checks input schemas and modelled LOB/peril lookup coverage. Expected
files, columns, dtypes, and required flags come from the colocated
[`schema.yaml` contracts](schema-contracts.md). Read the output in four
sections:

1. `Validation report`: schema, required-column, and type checks. `valid=False`
   means fix the file format before running.
2. `Modelled LOB/peril anti-join report`: should be empty. Any rows are blocking
   errors; add/fix values in `lobs.csv`/`perils.csv` or correct the input data.
3. `YLT loss validation summary`: non-blocking sanity totals unless an input read
   failed. Check file names, loss sums, and scaled loss.
4. `Input YLT AAL by LOB/peril summary`: raw input YLT AAL by vendor,
   rollup/modelled LOB, and rollup/modelled peril before blending, FX, forecast,
   or EUWS adjustments.

## 4. Run

```bash
uv run rollup run
```

Outputs land in root `output/`, not `data/output/`.

## 5. Optional: push marts to SQL Server

Copy `rollup.example.toml` to `rollup.local.toml`, fill in `[sql]`, and keep the
local file uncommitted. Check the connection first:

```bash
uv run rollup sql-check --config rollup.local.toml
```

To run the pipeline and push only `output/marts/*.parquet`:

```bash
uv run rollup run --push-sql --config rollup.local.toml
```

## 6. Inspect outputs

```bash
duckdb -c "SELECT COUNT(*) FROM 'output/mts_tbl_ylt_combined_all_factors.parquet';"
duckdb -c "SELECT * FROM 'output/mts_event_validation.parquet' LIMIT 20;"
```

## 7. Debug if needed

```bash
uv run rollup run --debug
duckdb -c "SELECT * FROM 'output/debug/int_ylt_blending_applied.parquet' LIMIT 10;"
```

## 8. Regenerate EP report explicitly

```bash
uv run rollup analyze
duckdb -c "SELECT * FROM read_csv_auto('output/analysis/ep_report.csv') LIMIT 20;"
```
