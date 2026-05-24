# Quickstart

Run from the repository root. If you are on a fresh Windows machine, first use
the [Windows install guide](windows-install.md) to install `uv` and build the
local environment.

## Step 1. Drop the data

Put analyst inputs here:

```text
data/ylt/verisk/*.parquet
data/ylt/risklink/*.parquet
data/ep_summaries/verisk/verisk_ep_summary.long.csv
data/ep_summaries/risklink/rms_ep_summary.long.csv
data/seeds/**
```

Do not put analyst inputs in `output/`.

Need to convert a vendor CSV to `.long.csv`?

1. Put the source CSV in `data/ep_summaries/<vendor>/`.
2. Run one of these commands:

```bash
uv run rollup generate-ep-summaries
uv run rollup generate-ep-summaries --vendor verisk --csv verisk_clean.csv --yes
```

3. Check the output file:
   - `data/ep_summaries/verisk/verisk_ep_summary.long.csv`
   - `data/ep_summaries/risklink/rms_ep_summary.long.csv`
4. Continue to Step 3 and validate.

For required source columns and output columns, see
[Creating EP summary long CSVs from wide CSVs](data-requirements.md#creating-ep-summary-long-csvs-from-wide-csvs).

## Step 2. Check seed lookups

Check these files before validation:

- `data/seeds/business/lobs.csv`: must contain every EP/YLT modelled LOB; maps
  to rollup LOB, class, office, currency, and CDS class metadata.
- `data/seeds/business/perils.csv`: must contain every EP/YLT modelled peril;
  maps to rollup peril, region/peril labels, `region_peril_id`, and
  `selection_priority`.

## Step 3. Validate

```bash
uv run rollup validate
```

Optional: write validation reports to CSV:

```bash
uv run rollup validate --report-dir output/validation
```

Validation should pass before you run the pipeline. Read the output in four
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

## Step 4. Run

```bash
uv run rollup run
```

Outputs land in root `output/`.

## Step 5. Inspect outputs

```bash
duckdb -c "SELECT COUNT(*) FROM 'output/mts_tbl_ylt_combined_all_factors.parquet';"
duckdb -c "SELECT * FROM 'output/mts_event_validation.parquet' LIMIT 20;"
```

## Step 6. Debug if needed

```bash
uv run rollup run --debug
duckdb -c "SELECT * FROM 'output/debug/int_ylt_blending_applied.parquet' LIMIT 10;"
```

## Step 7. Generate EP report

```bash
uv run rollup analyze
duckdb -c "SELECT * FROM read_csv_auto('output/analysis/ep_report.csv') LIMIT 20;"
```
