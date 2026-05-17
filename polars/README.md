# YAML-backed pipeline2

This repository is now intentionally centered on the pipeline2 path:

- `polars/rollup/pipeline2.py` — the small Polars DAG.
- `polars/rollup/pipeline2_schema.py` — YAML manifest loading and column validation.
- `data/seeds/schema.yaml`, `data/ylt/schema.yaml`,
  `data/ep_summaries/schema.yaml`, and `data/output/schema.yaml` — the
  data-side source of truth for input, staging, intermediate, and mart schemas.

The old runtime, CLI, docs, scripts, and tests were deleted so there is no
parallel path to confuse with pipeline2.

## Current flow

Pipeline2 loads YAML-declared sources, validates their columns strictly, stages
RiskLink and Verisk YLT rows into a minimal canonical shape, filters to selected
analyses, and produces a loss-summary mart.

```
data/seeds/schema.yaml + data/ylt/schema.yaml
        │
        ▼
load + validate sources
        │
        ▼
stage normalized YLT rows
        │
        ▼
filter selected losses
        │
        ▼
loss-summary mart validated by data/output/schema.yaml
```

## Run tests

```bash
uv run pytest polars/tests/test_pipeline2.py \
  polars/tests/test_pipeline2_schema.py \
  polars/tests/test_pipeline2_schema_yaml.py -q

uv run pytest -q
```
