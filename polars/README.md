# Clean pipeline Polars rollup

This repository now exposes the pipeline path only: a small, schema-driven
Polars DAG with dbt-style model folders.

## Where things live

```text
data/seeds/schema.yaml                 # seed/source schema manifest
data/ylt/schema.yaml                   # YLT source and YLT-derived model schemas
data/ep_summaries/schema.yaml          # optional EP summary schema manifest
data/output/schema.yaml                # output mart schema manifest
polars/rollup/pipeline_schema.py      # minimal YAML loader/validator helper
polars/rollup/pipeline.py             # orchestration only
polars/rollup/staging/pipeline.py     # staging model query functions
polars/rollup/intermediate/pipeline.py# intermediate model query functions
polars/rollup/marts/pipeline.py       # mart model query functions
```

`pipeline.py` merges the colocated manifests, loads and preflights source
schemas at the boundary, then calls the staging, intermediate, and mart query
functions in that order.

## Run tests

```bash
uv run pytest -q
```
