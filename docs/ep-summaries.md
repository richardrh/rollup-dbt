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

The pipeline expects `.long.csv` files under vendor-named subdirectories of
`data/ep_summaries/`:

```text
data/ep_summaries/
├── verisk/
│   └── verisk_ep_summary.long.csv
└── risklink/
    └── rms_ep_summary.long.csv
```

Only files matching `*.long.csv` are read during pipeline validation and
staging. Non-long CSV files (e.g. source wide CSVs) are ignored.

## Creating long CSVs from wide CSV exports

When an analyst or vendor provides a **wide** CSV (one row per analysis, with
EP losses in metric columns), use `rollup generate-ep-summaries` to convert it.

### Step 1. Place the source CSV

```text
data/ep_summaries/verisk/verisk_clean.csv
data/ep_summaries/risklink/risklink_clean.csv
```

### Step 2. Run the converter

Interactive — picks up all unmatched source CSVs:

```bash
uv run rollup generate-ep-summaries
```

Non-interactive — for a specific vendor and file:

```bash
uv run rollup generate-ep-summaries --vendor verisk --csv verisk_clean.csv --yes
```

### Step 3. Verify the output

The converter writes a `.long.csv` to the vendor folder:

```text
data/ep_summaries/verisk/verisk_ep_summary.long.csv
data/ep_summaries/risklink/rms_ep_summary.long.csv
```

### Step 4. Validate

```bash
uv run python -m rollup run --data-root data --output-root output --target-currency GBP --no-stage-outputs --no-analysis
```

The validation step checks that all EP summary LOBs and perils exist in the
business seed files. Fix any anti-join failures before running the pipeline.

## How the pipeline uses EP summaries

The pipeline stages, enriches, and blends EP summaries during the run:

1. **Staging** — reads `.long.csv` files, enriches with `rollup_lob` and
   `rollup_peril` from the business seed files, selects the preferred modelled
   peril for each `rollup_lob` + `rollup_peril` combination.
2. **Blending** — creates blended EP loss targets from Verisk and RiskLink
   sources using VOR blending weights.
3. **EP report** — after the mart fanout, the pipeline writes
   `output/analysis/ep_report.csv` with per-forecast-date EP losses, ranked
   and bucketed alongside YLT results.

## Pipeline outputs

The final EP report is written to `output/analysis/ep_report.csv` by both
`run_rollup(..., write_analysis=True)` or a normal CLI run:

```bash
uv run rollup run       # full pipeline — includes EP report
uv run python -m rollup run --data-root data --output-root output --target-currency GBP
```

The report contains 2,100 rows (typical) with columns:

| Column | Description |
| --- | --- |
| `forecast_date` | Forecast date |
| `metric` | `main` or `dialsup` |
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
