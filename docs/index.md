# Rollup Pipeline

The rollup pipeline converts Verisk and RiskLink catastrophe YLTs, EP summaries,
and business seed data into Hiscox mart parquets and optional analysis exports.
The current runtime is Dataiku-first, with a small local CLI for validation and
smoke testing.

## Start here

```bash
uv run python -m rollup run --data-root data --output-root output --target-currency GBP
uv run python -m rollup run --data-root data --output-root output --target-currency GBP --no-stage-outputs --no-analysis
uv run python -m rollup run --data-root data --output-root output --target-currency GBP --duckdb
```

Logs default to `output/rollup.log`. The CLI summary prints absolute output
paths, mart parquet counts, analysis status, stage-output status, and DuckDB
status.

## Main outputs

```text
output/
  marts/
    mts_tbl_ylt_combined_all_factors.parquet
    mts_tbl_ylt_combined_all_factors_wide.parquet
    mts_tbl_ylt_dialsup.parquet
    mts_event_validation.parquet
    HiscoAIR_..._main.parquet
    HiscoRMS_..._main.parquet
  stages/
    staging/
    intermediate/
  analysis/
    ep_report.csv
  rollup.log
```

Use `--no-stage-outputs` to skip `stages/`. Use `--no-analysis` to skip
`analysis/ep_report.csv`.

## Documentation map

- [Runtime guide](runtime.md) — end-to-end runtime, calculation flow, output
  contracts, DuckDB, smoke values, and known follow-ups.
- [Data requirements](data-requirements.md) — required input folder layout and
  seed files.
- [Calculation reference](calculation-reference.md) — EP selection, blending,
  FX, forecast, EUWS, DIALSUP, and metrics.
- [Programmatic API](programmatic-api.md) — Python entry points for Dataiku and
  other callers.
- [Troubleshooting](troubleshooting.md) — common validation and runtime issues.

Older guides remain for analyst setup, EP-summary conversion, Windows install,
and bundle-building workflows.
