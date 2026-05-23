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

## Build the standalone CLI bundle

The PyInstaller build is managed by `uv`. Build from the repository root:

```bash
uv run --group build pyinstaller rollup.spec
```

The output is a one-folder distribution under `dist/rollup/`; `dist/` is ignored
and not committed. Smoke test the executable after each build:

```bash
dist/rollup/rollup --help
dist/rollup/rollup generate-ep-summaries --help
dist/rollup/rollup docs
```

The bundle includes the project docs, `zensical.toml`, and Zensical assets so the
docs command can run without `uv` or an external `zensical` executable.
