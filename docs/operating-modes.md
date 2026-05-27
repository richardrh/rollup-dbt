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
```

Writes mart fanouts to `output/marts/`, wide/report parquets to `output/`, and
`output/analysis/ep_report.csv`. The wide combined-all-factors parquet is
`output/mts_tbl_ylt_combined_all_factors_wide.parquet` with forecast loss
columns such as `main_YYYYMM_loss` and `dialsup_YYYYMM_loss`.

## SQL Server check and push

Copy `rollup.example.toml` to gitignored `rollup.local.toml`, then fill in the
`[sql]` connection string, schema, push mode, and optional table prefix.

Check SQL Server connectivity without running the pipeline:

```bash
uv run rollup sql-check --config rollup.local.toml
uv run rollup test-sql --config rollup.local.toml
```

Run locally and push only mart fanout parquets from `output/marts/*.parquet`:

```bash
uv run rollup run --push-sql --config rollup.local.toml
```

Root-level output parquets and non-parquet files are not pushed. Table names are
derived from mart filenames, optionally prefixed by `[sql].table_prefix`, and
validated as safe SQL identifiers before writing.

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

Also writes stage frames to `output/debug/` with prefixes:

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
uv run rollup docs --host localhost --port 8000
uv run rollup docs --foreground
```

The command starts Zensical docs in the background by default and prints the URL,
process ID, log path, and `kill <pid>` stop command. Use `--foreground` to keep
the docs server attached to the terminal. Direct Zensical use is also available:

```bash
uv run zensical serve --config-file zensical.toml --dev-addr localhost:8000
```
