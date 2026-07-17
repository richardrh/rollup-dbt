# Developer guide

## Add a new pipeline step

Use this checklist when changing the pipeline shape.

1. Add or modify one logical output model per file in the dbt-like Polars
   layout: `src/rollup/staging/`, `intermediate/`, or `marts/`. Sources own input
   loading/discovery; writers own output writing.
2. Expose only the public model API: `validate(...) -> None` and
   `transform(...) -> pl.LazyFrame`. `transform()` must call its own
   `validate()`. Do not add numbering, model base classes, registries, dynamic
   discovery, or context containers.
3. Use the schema-validation helpers for required columns, important dtype
   families, join-key compatibility, and output-plan schema resolution. Use
   `validate_output(model, frame)` for output-plan schema checks, then explicitly
   `return frame`. Keep any row-scanning checks (nulls, uniqueness, ranges,
   cardinality) in data-quality validation/tests, not model `validate()`.
4. Source adapters expose exactly one public operation: `load(data_root)`. The
   function must return lazy Polars scans without collecting rows; EP per-file
   schema resolution is allowed. Keep discovery and immediate source validation
   there, and do not add private helper functions or compatibility aliases.
5. Keep `src/rollup/pipeline.py` as orchestration only: import the model module
   and call `module.transform(...)` in the correct phase. Prefer `LazyFrame`; keep
   source IO in sources and file/DuckDB/subprocess IO in writers.
6. Add shared column names to `src/rollup/columns.py` enums instead of repeating
   string literals.
7. If the step adds or changes an input, output, stage, or mart contract, update
   the appropriate reference Validnator contract when applicable and add runtime
   validation/model tests as relevant.
8. Register every useful debug frame in `source_frames`, `seed_frames`,
   `staging_frames`, `intermediate_frames`, or `mart_frames` under its semantic
   suffix so the writer emits the correct `src_`, `seed_`, `stg_`, `int_`, or
   `mts_` prefix.
9. If it is a final, wide, fanout, debug, or DuckDB product output, update the
   relevant writer module and the explicit calls in `src/rollup/pipeline.py` or
   `src/rollup/cli.py`. Product writers expose `validate(...) -> None` and
   `write(...)`; do not add aggregate output wrappers or hidden filesystem
   discovery as an execution engine.
10. If it feeds the EP report, update `src/rollup/analysis.py`.
11. Add architecture/model validation tests and runtime tests in `tests/` using
    `tmp_path` synthetic data. Do not mutate production `data/`.
12. Update README/docs and any command examples.
13. Run:

```bash
uv run pytest -q
uv run rollup validate
uv run rollup run --debug
```

## Debug dictionary rule

If a frame is useful for analysts or future debugging, put it in the right layer
dictionary before returning from `run()`. Use the semantic suffix matching the
model, for example `ep_summaries` or `ylt_main_ranked`; the debug writer adds
`src_`, `seed_`, `stg_`, `int_`, or `mts_`. Otherwise `--debug` cannot write it
to `output/debug/`.

## Tests

The default unit suite does not require Docker or external services:

```bash
uv run pytest -q
```

See [Building packages](building.md) when you need to build a wheel for another
Python environment.
