# Pipeline2 layout

Pipeline2 uses a clean dbt-style source layout without the old runtime.

- Seed schema manifest: `data/seeds/schema.yaml`
- YLT schema manifest: `data/ylt/schema.yaml`
- EP summary schema manifest: `data/ep_summaries/schema.yaml`
- Output schema manifest: `data/output/schema.yaml`
- YAML helper: `polars/rollup/pipeline2_schema.py`
- Orchestration: `polars/rollup/pipeline2.py`
- Staging models: `polars/rollup/staging/pipeline2.py`
- Intermediate models: `polars/rollup/intermediate/pipeline2.py`
- Mart models: `polars/rollup/marts/pipeline2.py`

The orchestrator performs source loading and schema preflight before any model
transformations, then executes staging → intermediate → marts as a linear DAG.
