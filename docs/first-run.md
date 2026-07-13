# Quickstart

Run from the repository root. If you are on a fresh Windows machine, first use
the [Windows install guide](windows-install.md) to install `uv` and build the
local environment.

## Step 1. Drop the data

Put analyst inputs under `data/`:

```text
data/ylt/verisk/*.parquet
data/ylt/risklink/*.parquet
data/ep_summaries/verisk/verisk_ep_summary.long.csv
data/ep_summaries/risklink/rms_ep_summary.long.csv
data/seeds/**
```

Generated outputs land in root `output/`; do not put analyst inputs there.

YLT files must be Parquet. If a YLT extract arrives as CSV, convert it first
with DuckDB; see [Utilities](utilities.md#convert-a-ylt-csv-extract-to-parquet-with-duckdb)
for high-level guidance.
You can provide one or more YLT parquet files per vendor. The pipeline loads every
direct `*.parquet` file in `data/ylt/verisk/` and `data/ylt/risklink/`; there is
no required filename pattern beyond the extension. Use clear names, and do not
place inactive/test parquet files in those folders. Subdirectories are ignored.

EP summaries must be canonical long CSVs under `data/ep_summaries/**/*.long.csv`.
The normal files are `data/ep_summaries/verisk/verisk_ep_summary.long.csv` and
`data/ep_summaries/risklink/rms_ep_summary.long.csv`.

Required EP summary columns:

```text
vendor,analysis_id,modelled_lob,modelled_peril,ep_type,return_period,loss
```

Need to convert a vendor/source EP summary CSV to `.long.csv`? Put the source CSV
in `data/ep_summaries/<vendor>/`, then run either the interactive converter or a
specific non-interactive conversion:

```bash
uv run rollup generate-ep-summaries
uv run rollup generate-ep-summaries --vendor verisk --csv verisk_clean.csv --yes
```

Check that the converter wrote the expected `.long.csv`, then continue to Step 3
and validate. For source columns, aliases, and examples, see
[EP summaries](ep-summaries.md).

## Step 2. Check seed lookups

Check these files before validation:

- `data/seeds/business/lobs.csv`: must contain every EP/YLT modelled LOB; maps
  to rollup LOB, class, office, currency, and CDS class metadata.
- `data/seeds/business/perils.csv`: must contain every EP/YLT modelled peril;
  maps to rollup peril, region/peril labels, `region_peril_id`, and main-pipeline
  `selection_priority`. It also includes DIALSUP-only `is_dialsup`, which marks
  active base/least-adjusted DIALSUP candidates.

The anti-join validation only flags the reverse: input values missing from seed
files. Adding a seed entry without matching data will not cause errors, but it
will also not be used by the pipeline.

## Step 3. Validate

```bash
uv run rollup validate
```

Optional: write validation reports to CSV while preserving the same console
output:

```bash
uv run rollup validate --report-dir output/validation
```

This creates `validation_report.csv`,
`modelled_lob_peril_anti_join_report.csv`,
`ylt_loss_validation_summary.csv`, and
`input_ylt_aal_by_lob_peril_summary.csv` under `output/validation/`.

Validation checks input schemas and modelled LOB/peril lookup coverage. Expected
files, columns, dtypes, and required flags come from the colocated validnator
contracts under `data/`. Read the output in four sections:

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

Outputs land in root `output/`, not `data/output/`. DuckDB export is enabled by
default and writes `output/rollup.duckdb`; pass `--no-duckdb` when you only need
parquet outputs. Final main and DIALSUP mart rows use the configured minimum
event loss threshold.

Open the DuckDB export directly when you need to inspect outputs:

```bash
duckdb output/rollup.duckdb
duckdb output/rollup.duckdb < sql/01_inventory.sql
```

## Step 5. Inspect outputs

Start with the DuckDB analyst templates:

```bash
duckdb output/rollup.duckdb < sql/01_inventory.sql
duckdb output/rollup.duckdb < sql/02_ep_report.sql
```

For LOB/peril checks, edit the `CHANGE_ME` filters in
`sql/03_mts_wide_lob_peril_waterfall.sql` or
`sql/04_mts_long_lob_peril_waterfall.sql`, then run the script against
`output/rollup.duckdb`. The long-table template is the clearest way to follow
the transform path from original loss through blending, FX, forecast, EUWS, and
final override.

## Step 6. Debug if needed

```bash
uv run rollup run --debug
```

Then inspect the debug frames under `output/debug/`, starting with the YLT
blending output.

## Step 7. Generate EP report

```bash
uv run rollup analyze
```

Review `output/analysis/ep_report.csv` after generation.
