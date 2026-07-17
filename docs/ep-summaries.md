# EP summaries

EP (Exceedance Probability) summaries are a key input to the pipeline. They
provide vendor-modelled loss exceedance data that the pipeline enriches, blends,
and combines with YLT data to produce final outputs.

## Required format

EP summaries must be **long CSVs** with exactly these columns:

```text
vendor,analysis_id,modelled_lob,modelled_peril,ep_type,return_period,loss
```

| Column | Description |
| --- | --- |
| `vendor` | Fixed value set by the vendor folder (`verisk` or `risklink`) |
| `analysis_id` | Analysis identifier from the source |
| `modelled_lob` | Modelled line of business — must exist in `lobs.csv` |
| `modelled_peril` | Modelled peril — must exist in `perils.csv` |
| `ep_type` | EP metric: `AAL`, `AEP`, or `OEP` |
| `return_period` | Return period as an integer (`0` for AAL) |
| `loss` | Loss value as a float |

## File locations

The pipeline requires at least one `.long.csv` file under both canonical vendor
roots in `data/ep_summaries/`:

```text
data/ep_summaries/
├── verisk/
│   └── verisk_ep_summary.long.csv
└── risklink/
    └── rms_ep_summary.long.csv
```

Only files matching `*.long.csv` are read during pipeline validation and
pipeline runs. They may be nested below the canonical `verisk/` or `risklink/`
root. Each individual long file must contain the exact canonical columns listed
above. Vendor is derived from the canonical root and overwrites any in-file value.
Non-long CSV files (e.g. source wide CSVs) are ignored. Unknown, root-level, and
case-variant vendor folders are rejected; vendor folders named `vendor=...` are
not supported.

## Creating long CSVs from wide CSV exports

When an analyst or vendor provides a **wide** CSV (one row per analysis, with
EP losses in metric columns), use `rollup generate-ep-summaries` to convert it.

### Step 1. Place the source CSV

```text
data/ep_summaries/verisk/verisk_clean.csv
data/ep_summaries/risklink/risklink_clean.csv
```

The wide converter requires exact source columns `id`, `modelled_lob`, and
`modelled_peril`, plus at least one metric column named exactly `AAL_0`,
`AEP_<integer>`, or `OEP_<integer>`. Lowercase metric names, `.0` suffixes, and
alternate LOB/peril column names are rejected.

### Step 2. Run the converter

Interactive — picks up all unmatched source CSVs:

```bash
uv run rollup generate-ep-summaries
```

Interactive mode prompts you to choose a vendor and candidate source file.

Non-interactive — for a specific vendor and file:

```bash
uv run rollup generate-ep-summaries --vendor verisk --csv verisk_clean.csv
```

The programmatic batch operation `generate_ep_summaries(data_root)` requires
exactly one candidate wide source CSV per vendor. Zero or multiple candidates for
any vendor fail; when multiple files exist, select explicitly with
`generate_vendor_ep_summary(...)` or CLI `--vendor` plus `--csv`.

### Step 3. Verify the output

The converter writes a `.long.csv` to the vendor folder:

```text
data/ep_summaries/verisk/verisk_ep_summary.long.csv
data/ep_summaries/risklink/rms_ep_summary.long.csv
```

### Step 4. Validate

```bash
uv run rollup validate
```

The validation step checks that all EP summary LOBs and perils exist in the
business seed files. Fix any anti-join failures before running the pipeline.

## How the pipeline uses EP summaries

The pipeline reads EP summaries with `sources/ep_summaries.load(data_root)` and
feeds the lazy combined frame directly into `int_ep_summaries_enriched`; there is
no pass-through staging model. During a run:

1. **Enrichment** — reads `.long.csv` files, validates canonical columns plus
   numeric `return_period`/`loss`, enriches with `rollup_lob` and
   `rollup_peril` from the business seed files, selects the preferred modelled
   peril for each `rollup_lob` + `rollup_peril` combination.
2. **Blending** — creates blended EP loss targets from Verisk and RiskLink
   sources using VOR blending weights.
3. **EP report** — after the long product outputs are written, the pipeline writes
   `output/analysis/ep_report.csv` with per-forecast-date EP losses, ranked
   and bucketed alongside the final main and DIALSUP YLT results. The report uses
   the effective runtime config for simulation counts and return periods.

## Pipeline outputs

The final EP report is written to `output/analysis/ep_report.csv` by
`rollup run`:

```bash
uv run rollup run       # full pipeline — includes EP report
```

When `rollup run` writes `output/rollup.duckdb`, this CSV is also available as
the `ep_report` table. Use `sql/02_ep_report.sql` for standard DuckDB inspection
queries.

| Column | Description |
| --- | --- |
| `forecast_date` | Forecast date |
| `metric` | Final metric name, currently `euws_override` or `dialsup_localccy_forecast` |
| `ep_type` | `AAL`, `AEP`, or `OEP` |
| `return_period` | Return period |
| `base_model` | Vendor base model |
| `rollup_lob` | Rollup LOB |
| `rollup_peril` | Rollup peril |
| `rank` | Rank within bucket |
| `rp` | Return period year value |
| `loss` | Loss value |

## See also

- [Data requirements](data-requirements.md#creating-ep-summary-long-csvs-from-wide-csvs) —
  full reference for the wide-to-long converter
- [Calculation reference](calculation-reference.md) — how EP summaries are
  selected, joined, and used for blending
- [Adding LOBs and perils](adding-lobs-perils.md) — how to register new LOBs
  and perils in the business seed files
- [Operating modes](operating-modes.md) — running the pipeline and analysis
