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

Writes mart fanouts to `output/marts/` and wide/report parquets to `output/`.

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

Generates `output/analysis/ep_report.csv` from pipeline outputs.

## Serve docs

```bash
uv run rollup docs
uv run rollup docs --host 127.0.0.1 --port 8000
```

The command serves Zensical docs and prints a URL. Direct Zensical use is also
available:

```bash
uv run zensical serve --config-file zensical.toml --dev-addr 127.0.0.1:8000
```
