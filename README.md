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
uv run rollup docs
```

Use `uv run rollup docs --host localhost --port 4322` when you need a fixed docs
address. The docs include the full [Quickstart](docs/first-run.md), operating
modes, troubleshooting, and developer notes.

Outputs are written to `output/`. The default run also writes
`output/rollup.duckdb` for local inspection with the SQL templates under `sql/`.

## Build a Python package

Use the standard Python package build when you need a wheel for another Python
environment:

```bash
uv build
```

See [Building](docs/building.md) for wheel install and import smoke checks.
