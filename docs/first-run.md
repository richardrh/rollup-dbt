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

## 2. Validate

```bash
uv run rollup validate
```

Validation checks input schemas and modelled LOB/peril lookup coverage. Fix any
missing-input or anti-join failures before running the pipeline.

## 3. Run

```bash
uv run rollup run
```

Outputs land in root `output/`, not `data/output/`.

## 4. Inspect outputs

```bash
duckdb -c "SELECT COUNT(*) FROM 'output/mts_tbl_ylt_combined_all_factors.parquet';"
duckdb -c "SELECT * FROM 'output/mts_event_validation.parquet' LIMIT 20;"
```

## 5. Debug if needed

```bash
uv run rollup run --debug
duckdb -c "SELECT * FROM 'output/debug/int_ylt_blending_applied.parquet' LIMIT 10;"
```

## 6. Generate EP report

```bash
uv run rollup analyze
duckdb -c "SELECT * FROM read_csv_auto('output/analysis/ep_report.csv') LIMIT 20;"
```
