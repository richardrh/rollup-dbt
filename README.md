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

Dataiku recipes and Python callers should use the package API:

```python
from rollup.api import convert_ep_summary, run_rollup

convert_ep_summary(
    input_csv="data/ep_summaries/verisk/verisk_clean.csv",
    vendor="verisk",
    output_csv="data/ep_summaries/verisk/verisk_ep_summary.long.csv",
)

result = run_rollup(
    data_root="data",
    output_root="output",
    config_path="rollup.toml",
    write_analysis=True,
)
```

- Validnator owns input/schema validation before runtime execution.
- `run_rollup(...)` runs the pipeline, preserves runtime business invariants,
  writes the optional DuckDB export, and generates the optional analysis report.
  Dataiku callers should pass
  `config_path` explicitly for job-specific configs rather than relying on
  the repository default `config.toml` in the current working directory.
- `convert_ep_summary(...)` converts one wide EP summary CSV to canonical long
  rows, returning a Polars `DataFrame` and optionally writing a CSV.

Runtime errors, including missing files needed for execution and business
invariant failures, are not hidden.

`run_rollup(...)` returns all output paths via `result.outputs`, including
combined, wide, DIALSUP, mart fanouts, optional stage output
directory, and optional DuckDB file. See [Programmatic API](docs/programmatic-api.md)
for the Dataiku temp-workspace pattern.

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

`perils.csv` includes `base_model` for blend base-model selection,
`selection_priority` for the main branch, `is_dialsup` for the DIALSUP branch,
and `is_euws` for EUWS factor application.

## Default output layout

```text
output/
  marts/
    mts_tbl_ylt_combined_all_factors.parquet
    mts_tbl_ylt_combined_all_factors_wide.parquet
    mts_tbl_ylt_dialsup.parquet
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
`seed_euws_rate_factors`, and `seed_euws_rank_overrides`.

Not included: fanouts, stage/intermediate outputs, DIALSUP mart, and wide mart.

## Configuration

`config.toml` is tracked and loaded by default. Pass `config_path` explicitly
for local or job-specific TOML files, including any `rollup.local.toml` override;
`rollup.local.toml` is no longer the runtime default. Supported keys are defined
in `src/rollup/config.py`.

```toml
[fx]
target_currency = "GBP"

[outputs]
write_stage_outputs = true
write_duckdb = false
duckdb_file = "rollup.duckdb"

[outputs.fanout_prefixes]
verisk = "HiscoAIR"
risklink = "HiscoRMS"

[analysis]
return_periods = [30, 200, 1000]

[vendor_years]
verisk = 10000
risklink = 100000

[blending]
uplift_factor_min = 0.1
uplift_factor_max = 10.0
target_points = [
    { ep_type = "AAL", return_period = 0 },
    { ep_type = "OEP", return_period = 200 },
    { ep_type = "OEP", return_period = 1000 },
]

[blending.subregion_selection]
"216" = "216b"
```

Vendor years control EP report rank/AAL calculations and YLT rank to
return-period bucket conversion during EP-derived blending. The blend target
points, uplift clipping bounds, VOR subregion selections, and fanout filename
prefixes are configuration defaults rather than hidden code branches.

## More documentation

- [Runtime guide](docs/runtime.md) — end-to-end flow, outputs, metrics, and known
  follow-ups.
- [Data requirements](docs/data-requirements.md) — input file and seed contracts.
- [Calculation reference](docs/calculation-reference.md) — blending, FX,
  forecast, EUWS, and DIALSUP details.
