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
implies an export. The exporter rebuilds the database from scratch by deleting
the existing DuckDB file before writing.

The DuckDB export is an analyst inspection bundle. It includes every
`output/**/mts_tbl_*.parquet` file as a separate table named from the parquet
stem, `output/analysis/ep_report.csv` as `ep_report` when present, and every
non-validation seed CSV under `data/seeds/**/*.csv` as `seed_<csv_stem>`. It
does not include raw input YLTs, validation files under `data/seeds/validation/`,
validation reports, mart fanout parquets under `output/marts/`, or `.rollup_work`
internals.

The main and DIALSUP fanouts include only final metric rows at or above the
configured minimum event loss threshold. The standalone DIALSUP parquet
`mts_tbl_ylt_dialsup.parquet` contains only final `dialsup_localccy_forecast` rows;
intermediate DIALSUP metrics are internal/debug data.

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

## SQL Server check and push

Copy `rollup.example.toml` to gitignored `rollup.local.toml`, then fill in the
`[sql]` connection string, schema, push mode, and optional table prefix.

Check SQL Server connectivity without running the pipeline:

```bash
uv run rollup sql-check --config rollup.local.toml
uv run rollup test-sql --config rollup.local.toml
```

`rollup run` writes files only. It no longer pushes marts to SQL Server as part
of the run command. Load `output/marts/*.parquet` through Dataiku or a separate
SQL-loading process after the pipeline finishes.

## Validation reports

```bash
uv run rollup validate --report-dir output/validation
```

Use `--report-dir` when you need to share validation evidence or attach outputs
to a ticket. The command still prints the normal console report and also writes
one CSV per validation table under the chosen directory.

## Debug run

```bash
uv run rollup run --debug
```

Writes stage frames to `output/debug/` with prefixes:

- `seed_*`
- `stg_*`
- `int_*`
- `mts_*`

## Analyze

```bash
uv run rollup analyze
```

Regenerates `output/analysis/ep_report.csv` from existing pipeline outputs
without rerunning the pipeline.

## Serve docs

```bash
uv run rollup docs
uv run rollup docs --host localhost --port 4322
```

The command starts Zensical docs in the foreground and prints the URL. Direct
Zensical use is also available:

```bash
uv run zensical serve --config-file zensical.toml --dev-addr localhost:4322
```
