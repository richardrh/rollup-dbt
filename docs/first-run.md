# Quickstart

Run from the repository root.

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

EP summaries consumed by the pipeline must be canonical long CSVs. If you have
source wide CSVs instead, put them in `data/ep_summaries/<vendor>/` and generate
the long files before validating:

```bash
uv run rollup generate-ep-summaries
uv run rollup generate-ep-summaries --vendor verisk --csv verisk_clean.csv --yes
```

Normal outputs are `data/ep_summaries/verisk/verisk_ep_summary.long.csv` and
`data/ep_summaries/risklink/rms_ep_summary.long.csv`. See
[Creating EP summary long CSVs from wide CSVs](data-requirements.md#creating-ep-summary-long-csvs-from-wide-csvs)
for the source CSV schema, metric naming, output columns, and why there is no
separate source-wide schema YAML.

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

## 5. Inspect outputs

```bash
duckdb -c "SELECT COUNT(*) FROM 'output/mts_tbl_ylt_combined_all_factors.parquet';"
duckdb -c "SELECT * FROM 'output/mts_event_validation.parquet' LIMIT 20;"
```

## 6. Debug if needed

```bash
uv run rollup run --debug
duckdb -c "SELECT * FROM 'output/debug/int_ylt_blending_applied.parquet' LIMIT 10;"
```

## 7. Generate EP report

```bash
uv run rollup analyze
duckdb -c "SELECT * FROM read_csv_auto('output/analysis/ep_report.csv') LIMIT 20;"
```
