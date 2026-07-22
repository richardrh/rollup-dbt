# Developer guide

## Add a new pipeline step

Use this checklist when changing the pipeline shape.

1. Add or modify one logical output model per file in the dbt-like Polars
   layout: `src/rollup/staging/`, `intermediate/`, or `marts/`. Sources own input
   loading/discovery; writers own output writing.
2. Define exactly one public `Model` class for each staging, intermediate, or
   mart module. Specialize `PolarsModel[P]` with the exact dependency argument
   types, where `P` is a `ParamSpec`. Implement only its abstract class methods,
   `schema()` and private `_transform(...)`, with `@override`. `schema()` is the
   exact ordered final output contract; final inherited `validate(frame)` and
   `transform(...)` are the public operations. Sources retain `load(data_root)`
   and writers retain their own `validate()`/`write()` contracts.
3. End `_transform()` with an explicit final `.select(...)`, including casts, in
   `schema()` order. Treat it like a SQL model's final `SELECT`: internal working
   columns stay upstream, while the selected columns are the published model
   boundary. Do not call validation in `_transform`: inherited `transform`
   always validates the returned lazy candidate.

   ```python
    from typing import override

    import polars as pl

    from rollup.model import PolarsModel


    class Model(PolarsModel[[pl.LazyFrame]]):
        @override
        @classmethod
        def schema(cls) -> pl.Schema:
            return pl.Schema({"id": pl.Int64, "amount": pl.Float64})

        @override
        @classmethod
        def _transform(cls, source: pl.LazyFrame) -> pl.LazyFrame:
            return source.select(
                pl.col("raw_id").cast(pl.Int64).alias("id"),
                pl.col("raw_amount").cast(pl.Float64).alias("amount"),
            )
   ```

   Call `module.Model.transform(...)`, `module.Model.schema()`, or
   `module.Model.validate(frame)` as appropriate, passing dependencies
   explicitly; do not instantiate `Model`. Schema validation resolves the lazy
   plan with `collect_schema()` and compares the exact ordered schema. It is lazy
   metadata planning, not row-value validation: it does not execute rows, though
   schema resolution can access source metadata. Keep null, value, uniqueness,
   range, and cardinality checks in data-quality validation/tests. Mypy enforces
   inheritance, signatures, and `@override`; Ruff handles formatting and lint.
   Architecture tests cover discovery, class structure, and final projection
   properties that type checking cannot. Do not add numbering, wrappers,
   registries, factories, decorators, generated projections, dynamic discovery,
   or context containers.
4. Source adapters expose exactly one public operation: `load(data_root)`. The
   function must return lazy Polars scans without collecting rows; EP per-file
   schema resolution is allowed. Keep discovery and immediate source validation
   there, and do not add private helper functions or compatibility aliases.
5. Keep `src/rollup/pipeline.py` as orchestration only: import the model module
   and call `module.Model.transform(...)` in the correct phase. Prefer
   `LazyFrame`; keep source IO in sources and file/DuckDB/subprocess IO in writers.
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

All categories use local synthetic fixtures; integration tests use `tmp_path`
files and do not require external services or real data. They are not a
real-data smoke pipeline.

| Command | Runs |
| --- | --- |
| `uv run pytest -q` | Normal tests only; integration and fuzz tests are skipped. |
| `uv run pytest -q --run-integration` | Normal and integration tests. |
| `uv run pytest -q -m integration` | Integration tests only. |
| `uv run pytest -q --run-fuzz` | Normal and property-based fuzz tests. |
| `uv run pytest -q -m fuzz` | Fuzz tests only. |
| `uv run pytest -q --run-integration --run-fuzz` | All test categories. |

`-q` keeps pytest output concise. `--run-integration` and `--run-fuzz` opt their
respective categories into an otherwise normal run. `-m` is pytest's marker
selector, so `-m integration` or `-m fuzz` selects only that category rather than
also selecting normal tests. Target a module, test, or matching test names as
needed:

```bash
uv run pytest -q tests/test_ylt_source_staging.py
uv run pytest -q tests/test_model.py::test_transform_invokes_private_hook_and_validates_its_schema
uv run pytest -q -k schema
uv run pytest -q --run-integration tests/test_pipeline_e2e_validation.py
```

Run static and documentation checks before submitting changes:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src tests
uv run zensical build --config-file zensical.toml
```

The Azure Pipelines definition is `pipelines/azure-pipelines.yml`. It uses `uv`
directly, not `pip`; see `pipelines/README.md` for Azure setup notes.

See [Building packages](building.md) when you need to build a wheel for another
Python environment.
