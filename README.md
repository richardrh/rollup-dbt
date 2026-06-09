# Rollup pipeline

Dataiku-first catastrophe rollup pipeline for Verisk and RiskLink YLT data.
The system reads local `data/` inputs, applies EP-derived blending, FX,
forecast, and EUWS adjustments, then writes mart parquets and optional analysis
exports under `output/`.

The runtime code lives in `src/rollup/`. The local CLI is intentionally small:
it is mainly for developer smoke tests and analyst/Dataiku-user validation.

## Quickstart

```bash
uv run python -m rollup run --data-root data --output-root output --target-currency GBP
uv run python -m rollup run --data-root data --output-root output --target-currency GBP --no-stage-outputs --no-analysis
uv run python -m rollup run --data-root data --output-root output --target-currency GBP --duckdb
```

Logs default to `<output-root>/rollup.log`. Use `--log-file <path>` to write
somewhere else.

The CLI summary prints absolute paths for the data root, output root, log file,
mart outputs, parquet counts, analysis report status, stage output status, and
DuckDB status.

## Programmatic API

Dataiku recipes and Python callers should use:

```python
from rollup.api import run_rollup, validate_rollup_inputs

validation = validate_rollup_inputs("data")
validation.raise_for_errors()

result = run_rollup(
    data_root="data",
    output_root="output",
    validate=True,
    write_analysis=True,
)
```

- `validate_rollup_inputs(data_root)` validates required source availability and
  main input schema/nullability.
- `run_rollup(...)` runs validation, the pipeline, optional DuckDB export, and
  optional analysis report generation.

Expected validation failures return a `RollupValidationResult(is_valid=False)`.
The CLI catches `RollupValidationError`, prints friendly details, and exits `1`.
Unexpected errors are not hidden.

## Expected input layout

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

`perils.csv` includes both `selection_priority` for the main branch and
`is_dialsup` for the DIALSUP branch.

## Default output layout

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

Stage outputs are disabled by `--no-stage-outputs`. Analysis is disabled by
`--no-analysis`.

## Optional DuckDB export

DuckDB export is disabled by default. Enable it with:

```bash
uv run python -m rollup run --data-root data --output-root output --target-currency GBP --duckdb
uv run python -m rollup run --data-root data --output-root output --duckdb-file output/custom.duckdb
```

Or in config:

```toml
[outputs]
write_duckdb = true
duckdb_file = "rollup.duckdb"
```

The default path is `output/rollup.duckdb`.

Included tables: `mts_tbl_ylt_combined_all_factors`, `input_ylt_verisk`,
`input_ylt_risklink`, `input_ep_summaries`, `seed_lobs`, `seed_perils`,
`seed_blending_factors`, `seed_fx_rates`, `seed_forecast_factors`,
`seed_euws_rate_factors`, `seed_euws_rank_overrides`, `seed_verisk_events`, and
`seed_risklink_flood22_model_events`.

Not included: fanouts, stage/intermediate outputs, DIALSUP mart, and wide mart.

## Configuration

`rollup.local.toml` is loaded by default when present. Supported keys are defined
in `src/rollup/config.py`.

```toml
[fx]
target_currency = "GBP"

[outputs]
write_stage_outputs = true
write_duckdb = false
duckdb_file = "rollup.duckdb"

[analysis]
return_periods = [30, 200, 1000]
```

Simulation count defaults used by analysis are `verisk = 10000` and
`risklink = 100000`. They can be overridden with `[analysis.simulation_counts]`
or legacy `num_sims_verisk` / `num_sims_risklink` keys.

## More documentation

- [Runtime guide](docs/runtime.md) — end-to-end flow, outputs, metrics, and known
  follow-ups.
- [Data requirements](docs/data-requirements.md) — input file and seed contracts.
- [Calculation reference](docs/calculation-reference.md) — blending, FX,
  forecast, EUWS, and DIALSUP details.
