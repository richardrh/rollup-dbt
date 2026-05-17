# Clean pipeline2 Polars rollup

This repository now exposes the pipeline2 path only: a small, schema-driven
Polars DAG with dbt-style model folders.

## Where things live

```text
data/seeds/schema.yaml                 # seed/source schema manifest
data/ylt/schema.yaml                   # YLT source and YLT-derived model schemas
data/ep_summaries/schema.yaml          # optional EP summary schema manifest
data/output/schema.yaml                # output mart schema manifest
polars/rollup/pipeline2_schema.py      # minimal YAML loader/validator helper
polars/rollup/pipeline2.py             # orchestration only
polars/rollup/staging/pipeline2.py     # staging model query functions
polars/rollup/intermediate/pipeline2.py# intermediate model query functions
polars/rollup/marts/pipeline2.py       # mart model query functions
```

`pipeline2.py` merges the colocated manifests, loads and preflights source
schemas at the boundary, then calls the staging, intermediate, and mart query
functions in that order.

## Run tests

```bash
uv run pytest -q
```
