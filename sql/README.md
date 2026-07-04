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

The DuckDB export contains the analyst inspection tables, not every pipeline
input or debug artifact:

- `output/**/mts_tbl_*.parquet` as separate tables named from each file stem.
- `output/analysis/ep_report.csv` as `ep_report`, when present.
- Non-validation seed CSVs under `data/seeds/**/*.csv` as `seed_<csv_stem>`.

It intentionally excludes raw YLT inputs, validation files under
`data/seeds/validation/`, validation reports, mart fanout parquets under
`output/marts/`, and `.rollup_work` internals.

Files:

- `01_inventory.sql`: tables, columns, and row counts.
- `02_ep_report.sql`: EP report row counts, loss summaries, and top losses.
- `03_mts_wide_lob_peril_waterfall.sql`: one LOB/peril wide-table waterfall.
- `04_mts_long_lob_peril_waterfall.sql`: one LOB/peril long-table waterfall.
- `05_seed_lookup_checks.sql`: exported seed lookup sanity checks.
- `99_export_example.sql`: example CSV export pattern.

For the wide waterfall template, run `01_inventory.sql` first and replace the
example `YYYYMM` loss columns with the forecast-month columns present in your
export.
