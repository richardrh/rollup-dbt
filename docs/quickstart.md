# Quick start

1. Install the `uv` dependencies and activate your environment.
2. Run `python -m app.source_cli` to interactively register the sources you need. The CLI prompts for server/database/file path, shows the current registry, and regenerates the DLT config.
3. Execute your DLT pipeline (or `dlt pipeline run ...`) so the `catmodel.*` tables are populated.
4. Run `dbt seed` + `dbt run --select ylt_all_factors_long_from_cachetbl` to move through the dbt DAG.

Updating a source simply means rerunning step 2, letting the CLI rewrite `config/sources.toml`, and rerunning the DLT + dbt steps.
