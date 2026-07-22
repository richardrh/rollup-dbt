# Rollup pipeline

Catastrophe rollup pipeline that reads analyst-supplied seed data, vendor YLT
parquets, and EP summary CSVs from `data/`, then writes mart/report outputs to
root `output/`.

Active code lives in `src/rollup/`. The CLI implementation is in
`src/rollup/cli.py`.

## Source checkout quickstart

From the repository root:

```bash
uv sync
uv run rollup validate
uv run rollup run
```

Serve repository docs with the pinned development docs tool when you need a local
site. Zensical is a development dependency, not a rollup runtime dependency:

```bash
uv run zensical serve --config-file zensical.toml --dev-addr localhost:4322
```

The docs include the full [Quickstart](docs/first-run.md), operating modes,
troubleshooting, and developer notes.

## Model contract and tests

Pipeline models are stateless class-level operations: call
`module.Model.transform(...)` rather than creating an instance. `PolarsModel` keeps
validation and transform orchestration final, so a concrete `_transform(...)` only
builds its lazy frame; the inherited transform validates it automatically.

```bash
uv run pytest -q                    # normal tests
uv run pytest -q --run-integration  # normal plus synthetic integration tests
uv run pytest -q --run-fuzz         # normal plus property-based fuzz tests
```

See the [developer guide](docs/developer-guide.md) for all test selections and
quality commands. The forthcoming Azure definition will live at
`pipelines/azure-pipelines.yml`; the DevOps task will create it and will use `uv`
directly rather than `pip`.

Outputs are written to `output/`. The default run also writes
`output/rollup.duckdb` for local inspection with the SQL templates under `sql/`.

The installed runtime depends directly on DuckDB and Polars. Development tooling
includes pytest, Hypothesis, mypy, Ruff, and Zensical.

## Build a Python package

Use the standard Python package build when you need a wheel for another Python
environment:

```bash
uv build
```

See [Building](docs/building.md) for wheel install and import smoke checks.
