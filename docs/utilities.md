# Utilities

Small commands for preparing and inspecting rollup inputs.

## Convert a YLT CSV extract to Parquet with DuckDB

The rollup pipeline expects YLT inputs as Parquet files under `data/ylt/<vendor>/`.
If an analyst extract arrives as CSV, use DuckDB to convert it before validation.

Useful DuckDB references:

- [DuckDB documentation](https://duckdb.org/docs/)
- [CSV import](https://duckdb.org/docs/stable/data/csv/overview.html)
- [Parquet overview](https://duckdb.org/docs/stable/data/parquet/overview.html)

Run the conversion from the repository root, writing the Parquet file into the
appropriate vendor folder, then validate the converted file:

```bash
uv run rollup validate
```

Keep source CSVs outside `output/`; `output/` is reserved for generated pipeline
results.
