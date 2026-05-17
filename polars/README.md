# Clean pipeline2 Polars rollup

This repository now exposes the pipeline2 path only: a small, schema-driven
Polars DAG with dbt-style model folders.

## Where things live

```text
data/pipeline2/schema.yaml             # data-side schema manifest
polars/rollup/pipeline2_schema.py      # minimal YAML loader/validator helper
polars/rollup/pipeline2.py             # orchestration only
polars/rollup/staging/pipeline2.py     # staging model query functions
polars/rollup/intermediate/pipeline2.py# intermediate model query functions
polars/rollup/marts/pipeline2.py       # mart model query functions
```

`pipeline2.py` loads and preflights the source schemas at the boundary before it
calls the staging, intermediate, and mart query functions in that order.

## Run tests

```bash
uv run pytest -q
```
