# Pipeline layout

Pipeline uses a clean dbt-style source layout without the old runtime.

- Seed schema manifest: `data/seeds/schema.yaml`
- YLT schema manifest: `data/ylt/schema.yaml`
- EP summary schema manifest: `data/ep_summaries/schema.yaml`
- Output schema manifest: `data/output/schema.yaml`
- YAML helper: `polars/rollup/pipeline_schema.py`
- Orchestration: `polars/rollup/pipeline.py`
- Staging models: `polars/rollup/staging/pipeline.py`
- Intermediate models: `polars/rollup/intermediate/pipeline.py`
- Mart models: `polars/rollup/marts/pipeline.py`

The orchestrator performs source loading and schema preflight before any model
transformations, then executes staging → intermediate → marts as a linear DAG.
