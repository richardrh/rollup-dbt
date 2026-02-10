# Data sources & ingestion

Raw Modeling and EP inputs enter this repo through a small DLT + CLI workflow that keeps the dbt graph agnostic to the concrete source location. The high-level pieces are:

1. **Connector metadata + overrides** — `config/sources.toml` is the single source of truth. Connector templates live under `[connectors.<name>]` (type, label, defaults, and field definitions) and the concrete source overrides are recorded in the `sources` array. Edit this file by hand or use the CLI when you want to keep track of what changed.
2. **Interactive source registry** — run `python -m app.source_cli` (or `python -m app.source_cli configure_sources`). The CLI prompts for a logical name, picks one of the connector templates defined inside `config/sources.toml`, and asks for the values that change (host/database, file path, etc.). Every submission persists back to `config/sources.toml`, so the overrides stay version-controlled.
3. **DLT config generation** — `app/dlt_config.py` reads `config/sources.toml` and writes `.dlt/config.toml`. The generated file contains a `[runtime]` section plus `[sources.<connector>.<name>.credentials]` subsections with the concrete params your pipeline needs (and handles nested dictionaries for query options).
4. **DBT sources** — `models/sources/catmodel.yml` declares the canonical tables (`verisk_ylt`, `risklink_ylt`, `verisk_ep`, `risklink_ep`, etc.) that DLT should populate. dbt staging models (`stg_risklink__ylt`, `stg_verisk__ylt`, etc.) simply clean column names and rely on `{{ source('catmodel', '<table>') }}`.
5. **Materialization** — After DLT writes the tables, dbt builds the staging → intermediate → mart pipeline without extra wiring; every downstream model references the registered tables via the source YAML.

This approach lets you change hosts or file paths by rerunning the CLI while leaving dbt unchanged. Connector metadata and overrides live under `./config`, and `.dlt/config.toml` is always regenerated before the pipeline runs.
