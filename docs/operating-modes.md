# Operating modes

## With and without uv

From a checkout, use `uv run`:

```bash
uv run rollup validate
uv run rollup run
```

After installing the project and activating the virtual environment, `uv` is not
required:

```bash
rollup validate
rollup run
```

Product CLI commands are `run`, `generate-ep-summaries`, `validate`, and
`cleanup`. Global path/log options may be placed before the subcommand where the
parser supports them; `run`, `validate`, and `generate-ep-summaries` also accept
their supported path options on the subcommand.

## Normal run

```bash
uv run rollup run
uv run rollup --log-file output/run.log run
```

Writes mart fanouts to `output/marts/`, wide/report parquets to `output/`, and
`output/analysis/ep_report.csv`. The wide combined-all-factors parquet is
`output/mts_tbl_ylt_combined_all_factors_wide.parquet` with forecast loss
columns such as `euws_override_YYYYMM_loss` and
`dialsup_localccy_forecast_YYYYMM_loss`.

DuckDB export is on by default and writes `output/rollup.duckdb` unless
configured otherwise. Use `uv run rollup run --duckdb-file path/to/file.duckdb`
to choose a different path, or `uv run rollup run --no-duckdb` to disable it.
The CLI rejects `--no-duckdb` with `--duckdb-file` because an explicit file path
implies an export. The exporter stages the replacement database on the
destination filesystem and publishes it only after successful generation, so an
existing good DuckDB file survives pre-publication failures.

The DuckDB export is an analyst inspection database. It includes generated
`mts_tbl_*.parquet` files, all mart fanout parquets under `output/marts/`,
recursive CSV/parquet seeds including validation catalogue seeds, and
`output/analysis/ep_report.csv` as `ep_report` when present. It excludes raw YLT
  inputs. Pipeline-local `rollup-work-*` temporary directories are removed at the
  end of each run.

The main and DIALSUP fanouts include only final metric rows at or above the
configured minimum event loss threshold. The DIALSUP parquet
`mts_tbl_ylt_dialsup.parquet` contains only final `dialsup_localccy_forecast` rows;
intermediate DIALSUP metrics are internal/debug data.

## Configuration

`rollup run` uses `config.toml` by default. To use another file, pass `--config`
on the run subcommand:

```bash
uv run rollup run --config path/to/config.toml
```

Supported configuration is limited to analysis return periods, vendor simulation
year counts, blending clamp/target settings, DuckDB enable/path, the minimum
event loss threshold, and logging format. Runtime output names, output
directories, mart fanout locations, staging/debug naming, and GBP-to-local FX
behaviour are fixed contracts rather than config knobs.

The TOML schema is strict. Unknown sections or keys fail, spelling/casing must
match exactly, and values must use the canonical runtime types:

```toml
[outputs]
write_duckdb = true
duckdb_file = "rollup.duckdb"
minimum_event_loss_threshold = 1000.0

[logging]
format = "jsonl" # "text" is also valid

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
]
```

If `[vendor_years]` is supplied, it must contain both `verisk` and `risklink`.
Logging format is exactly `text` or `jsonl`.

Use global `--log-file` before the subcommand to keep an operational run log
while still printing the same logs to the console/stdout. Parent directories are
created automatically:

```bash
uv run rollup --log-file output/run.log run
uv run rollup --log-file output/validate.log validate
```

Logging and debug output are separate controls:

- `--debug` on `rollup run` writes intermediate parquet frames under
  `output/debug/` for data inspection.
- `--log-level DEBUG` increases log verbosity.
- `--log-file output/run.log` writes logs to a file as well as stdout.

## Validation reports

```bash
uv run rollup validate --report-dir output/validation
uv run rollup --data-root /path/to/data validate
uv run rollup validate --data-root /path/to/data
```

Use `--report-dir` when you need to share validation evidence or attach outputs
to a ticket. The command writes the modelled LOB/peril anti-join report and the
input YLT AAL summary when they can be computed. Missing required inputs or seeds
return non-zero with concise stderr; modelled LOB/peril anti-join rows are
blocking, while input YLT AAL is informational.

## Debug run

```bash
uv run rollup run --debug
```

Writes debug frames to `output/debug/` with prefixes:

- `src_*`
- `seed_*`
- `stg_*`
- `int_*`
- `mts_*`

## Serve docs

```bash
uv run zensical serve --config-file zensical.toml --dev-addr localhost:4322
```

Zensical is a pinned development dependency for repository documentation. It is
not a runtime dependency of the installed rollup product CLI.

## Cleanup

```bash
uv run rollup cleanup
uv run rollup cleanup --yes
```

Without `--yes`, cleanup prompts before removing generated outputs. The `--yes`
flag is only for cleanup; EP-summary generation has no `--yes` option.
