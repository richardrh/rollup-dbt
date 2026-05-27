# Developer guide

## Add a new pipeline step

Use this checklist when changing the pipeline shape.

1. Add or modify a pure transformation function in `src/rollup/pipeline.py`.
   Prefer `LazyFrame`; keep file IO at the edges.
2. Add shared column names to `src/rollup/columns.py` enums instead of repeating
   string literals.
3. If the step adds or changes an input, output, stage, or mart contract, update
   the appropriate colocated `schema.yaml` and validation tests.
4. Call the function in the correct `run()` phase: validation, staging,
   intermediate, or marts.
5. Add the result to `seed_frames`, `staging_frames`, `intermediate_frames`, or
   `mart_frames` so `--debug` writes it with the right prefix.
6. If it is a final/wide/mart output, update `write_mart_outputs` or the relevant
   writer so it is written under `output/`.
7. If it feeds `rollup analyze`, update `src/rollup/analysis.py`.
8. Add tests in `tests/` using `tmp_path` synthetic data. Do not mutate
   production `data/`.
9. Update README/docs and any command examples.
10. Run:

```bash
uv run pytest -q
uv run rollup validate
uv run rollup run --debug
uv run rollup analyze
```

## Debug dictionary rule

If a frame is useful for analysts or future debugging, put it in the right stage
dictionary before returning from `run()`. Otherwise `--debug` cannot write it to
`output/debug/`.

## Integration tests

The default unit suite does not require Docker or external services:

```bash
uv run pytest -q
```

SQL Server push has an opt-in integration test that starts a Microsoft SQL Server
container, writes a tiny mart parquet via `push_mart_parquets_to_sql`, and reads
the table back through SQLAlchemy. The test uses the dev-only `pymssql` driver
when available and falls back to a local Microsoft ODBC driver if needed:

```bash
uv run pytest tests/test_sql_integration.py --run-integration -q -rs
```

The test skips with a clear reason when Docker or a usable SQL Server driver is
not available on the host.

## Build the standalone CLI bundle

See [Building the standalone bundle](building.md) for the PyInstaller build
process, what gets bundled, and smoke-testing the `dist/rollup/` output.
