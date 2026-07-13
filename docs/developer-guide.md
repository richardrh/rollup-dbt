# Developer guide

## Add a new pipeline step

Use this checklist when changing the pipeline shape.

1. Add or modify the model in the dbt-like Polars layout:
   `src/rollup/sources/`, `staging/`, `intermediate/`, `marts/`, or `writers/`.
   Keep `src/rollup/pipeline.py` as orchestration only. Prefer `LazyFrame`; keep
   file IO in sources or writers.
2. Add shared column names to `src/rollup/columns.py` enums instead of repeating
   string literals.
3. If the step adds or changes an input, output, stage, or mart contract, update
   the appropriate validnator contract and validation tests.
4. Call the function in the correct `run()` phase: validation/sources, staging,
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

## Tests

The default unit suite does not require Docker or external services:

```bash
uv run pytest -q
```

See [Building packages](building.md) when you need to build a wheel for another
Python environment.
