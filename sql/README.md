# Analyst SQL Templates

Reusable DuckDB queries for inspecting the default rollup export at
`output/rollup.duckdb`.

Run from the repository root:

```bash
duckdb output/rollup.duckdb < sql/01_inventory.sql
```

Or open DuckDB and paste individual queries:

```bash
duckdb output/rollup.duckdb
```

Final output metrics:

- Main metric: `euws_override` in `mts_tbl_ylt_combined_all_factors`.
- DIALSUP metric: `dialsup_localccy_forecast` in `mts_tbl_ylt_dialsup`.

Files:

- `01_inventory.sql`: tables, columns, and row counts.
- `02_portfolio_summaries.sql`: final main/DIALSUP portfolio summaries.
- `03_top_losses.sql`: largest final events.
- `04_waterfall_and_wide_checks.sql`: metric waterfall and wide output checks.
- `05_fanout_audit.sql`: CDS fanout counts and largest fanout events.
- `06_input_seed_qa.sql`: input EP and seed sanity checks.
- `99_export_example.sql`: example CSV export pattern.
