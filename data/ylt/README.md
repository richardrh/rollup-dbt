# data/ylt

Rollup reads production YLT inputs as parquet from the vendor subfolders.
Validnator currently validates one CSV input file at a time; it does **not**
validate parquet files directly. The YAML files in this folder are
CSV-equivalent checks for source extracts before, or alongside, conversion to
parquet.

Use the vendor-specific config that matches the CSV extract being validated:

```bash
uv run validnator validate -p data/ylt/validnator-verisk.yml -i exports/verisk_ylt.csv -o validation-output/verisk-ylt
uv run validnator validate -p data/ylt/validnator-risklink.yml -i exports/risklink_ylt.csv -o validation-output/risklink-ylt
```

Do not treat those runs as evidence that `data/ylt/**/*.parquet` has been
validated by validnator. Parquet YLT inputs are guarded at runtime by rollup's
Polars schema checks until validnator supports parquet inputs.
