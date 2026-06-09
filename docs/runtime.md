# Runtime guide

This page explains how the current rollup system works end-to-end. The runtime
is Dataiku-first, with a small local CLI for development, smoke testing, and
analyst operation outside Dataiku.

## Runtime entry points

```mermaid
flowchart TD
  A[Dataiku recipe or Python caller] --> B[run_rollup]
  C[Local CLI: python -m rollup run] --> B
  B --> D[validate_rollup_inputs]
  D --> E[Pipeline stages]
  E --> F[Parquet marts]
  E --> G[Optional stage outputs]
  E --> H[Optional DuckDB export]
  E --> I[Optional analysis report]
```

### Programmatic API

- `run_rollup(data_root="data", output_root="output", ...)` runs validation, all
  calculation stages, optional DuckDB export, and optional analysis report
  generation.
- `validate_rollup_inputs(data_root)` checks source availability and required
  schemas/nullability for the main inputs without writing outputs.

### CLI examples

```bash
uv run python -m rollup run --data-root data --output-root output --target-currency GBP
uv run python -m rollup run --data-root data --output-root output --target-currency GBP --no-stage-outputs --no-analysis
uv run python -m rollup run --data-root data --output-root output --target-currency GBP --duckdb
```

Logs default to `<output-root>/rollup.log`, unless `--log-file` is supplied. The
success summary prints absolute paths, mart parquet counts, analysis report
status, stage output status, and DuckDB status.

## Inputs

```text
data/
  ylt/verisk/*.parquet
  ylt/risklink/*.parquet
  ep_summaries/**/*.long.csv
  seeds/business/lobs.csv
  seeds/business/perils.csv
  seeds/vor/blending_factors.csv
  seeds/vor/fx_rates.csv
  seeds/vor/forecast_factors.csv
  seeds/vor/euws_rate_factors.csv
  seeds/adjustments/euws_rank_overrides.csv
  seeds/validation/verisk_events.parquet
  seeds/validation/risklink_flood22_model_events.parquet
```

`perils.csv` must contain `selection_priority` and `is_dialsup`.
`selection_priority` chooses the main EP peril candidate; `is_dialsup` chooses
the DIALSUP peril candidate at rollup peril level.

## Output layout

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

- `--no-stage-outputs` disables `output/stages/` writes.
- `--no-analysis` disables `output/analysis/ep_report.csv`.

## Calculation flow

The pipeline executes these stages in order:

1. `load_sources` reads YLT parquets, EP long CSVs, business seeds, VOR seeds,
   adjustment seeds, and validation catalogues.
2. `normalize_ylt` converts Verisk and RiskLink YLTs to common YLT columns.
3. `stage_ep_summaries` joins LOB/peril seeds and selects the lowest
   `selection_priority` per `(vendor, rollup_lob, rollup_peril)`, while
   preserving `is_dialsup` at rollup peril level.
4. `build_enriched_ylt` enriches normalized YLT rows from the staged EP summary
   mapping. RiskLink raw YLT is keyed by analysis id, so modelled dimensions
   should come from EP summary enrichment.
5. `apply_blending` restores EP-derived blending from the old master: AAL and
   OEP 200/1000 target points, blend weights, target loss, base model, base model
   loss, clipped uplift factor `0.1..10`, and rank/RP bucket joins.
6. `apply_fx` converts blended loss to the explicit target currency.
7. `apply_forecast` cross-joins every forecast date and uses factor `1.0` when a
   class/office/date factor is absent.
8. `apply_euws` applies event-based EUWS factors and rank overrides.
9. `build_metric_long` creates the combined long metric mart.
10. `build_dialsup` creates DIALSUP from original YLT loss multiplied by FX and
    forecast factors, not EUWS-adjusted loss. Rows are selected by
    `is_dialsup == 1`.
11. `write_marts` writes combined, wide, DIALSUP, event-validation, and fanout
    parquet files.

## Metrics and lineage

Combined long metrics:

- `loss_original_ylt`
- `loss_blended`
- `loss_blended_fx_gbp`
- `loss_blended_fx_gbp_forecast`
- `loss_blended_fx_gbp_forecast_euws_override`

DIALSUP metric:

- `loss_dialsup_fx_gbp_forecast`

Changing the target currency changes the metric tag. For example, USD produces
metric names containing `_fx_usd_...`.

## Wide output contract

`mts_tbl_ylt_combined_all_factors_wide.parquet` is a true wide pivot of the
combined all-factors mart:

- It has no `metric`, `forecast_date`, or `loss` columns.
- Row dimensions are all non-measure dimensions, including `target_currency` and
  `is_dialsup`.
- Value columns are `{metric}_{forecast_date_without_hyphens}`.

Example columns:

- `loss_original_ylt_20260101`
- `loss_blended_20260101`
- `loss_blended_fx_gbp_20260101`
- `loss_blended_fx_gbp_forecast_20260101`
- `loss_blended_fx_gbp_forecast_euws_override_20260101`

DIALSUP remains in `mts_tbl_ylt_dialsup.parquet`; it is not included in the
combined wide mart.

## DuckDB export

DuckDB export is optional and disabled by default.

CLI flags:

- `--duckdb` writes the default database.
- `--duckdb-file <path>` writes a specific database path and also enables DuckDB.

Config:

```toml
[outputs]
write_duckdb = true
duckdb_file = "rollup.duckdb"
```

Default path: `output/rollup.duckdb`.

Tables included:

- `mts_tbl_ylt_combined_all_factors`
- `input_ylt_verisk`
- `input_ylt_risklink`
- `input_ep_summaries`
- `seed_lobs`
- `seed_perils`
- `seed_blending_factors`
- `seed_fx_rates`
- `seed_forecast_factors`
- `seed_euws_rate_factors`
- `seed_euws_rank_overrides`
- `seed_verisk_events`
- `seed_risklink_flood22_model_events`

Not included: fanouts, stage/intermediate outputs, DIALSUP mart, and wide mart.

## Validation behavior

- `validate_rollup_inputs(data_root)` validates source availability and
  schema/nullability for main inputs.
- Expected input/schema failures return
  `RollupValidationResult(is_valid=False)` plus a validation report.
- The CLI catches `RollupValidationError`, prints friendly details, and exits
  with code `1`.
- Unexpected errors are not hidden; they propagate.
- Pandera schemas enforce required non-null fields.
- Lazy YLT required-null checks use null-count aggregates rather than collecting
  the full source data.

## Reference smoke values

These values are reference smoke checks against real `./data`, not hard
guarantees. Small floating-point differences are expected.

Combined sums:

- `loss_original_ylt` ≈ `595,127,587,394.46`
- `loss_blended` ≈ `579,116,007,376.25`
- `loss_blended_fx_gbp` ≈ `577,222,053,036.84`
- `loss_blended_fx_gbp_forecast` ≈ `566,796,627,725.94`
- `loss_blended_fx_gbp_forecast_euws_override` ≈ `566,250,261,028.68`

EP AAL:

- main/EUWS ≈ `11,175,803.275055`
- DIALSUP ≈ `12,772,490.495922`

Calculations now match Jun6 master within float noise, while output shape and
metric names are modernized.

## Known issues and follow-ups

- `Pen` and `Cherish` RiskLink output rows currently have null `modelled_lob` and
  `modelled_peril` despite EP summaries containing `MGA_Pen` and `MGA_Cherish`.
  Likely follow-up: `build_enriched_ylt` drops those fields from RiskLink EP keys
  before joining by `analysis_id`.
- DIALSUP fanout files are not emitted separately.
- Add an explicit error when EP blending weights are missing for a required
  region peril.
