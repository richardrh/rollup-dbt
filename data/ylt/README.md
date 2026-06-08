# data/ylt

Rollup reads production YLT inputs as parquet from the vendor subfolders.
The YAML files in this folder describe the expected vendor YLT schema and can be
used in two ways.

## CSV extract validation with the validnator CLI

The local validnator CLI input loader accepts CSV inputs, so keep CLI examples to
CSV extracts. Use the vendor-specific config that matches the extract being
validated:

```bash
uv run validnator validate -p data/ylt/validnator-verisk.yml -i exports/verisk_ylt.csv -o validation-output/verisk-ylt
uv run validnator validate -p data/ylt/validnator-risklink.yml -i exports/risklink_ylt.csv -o validation-output/risklink-ylt
```

The `input:` blocks in these YAMLs are retained for that CSV CLI path. They are
not used when a Polars DataFrame is supplied programmatically.

## Parquet validation from Polars/Dataiku DataFrames

Validnator also supports already-loaded DataFrames. For parquet YLT files, load
the file with Polars (or receive the equivalent Dataiku DataFrame converted to
Polars) and call `Pipeline.run_with_df(df)`:

```python
from pathlib import Path

import polars as pl
from validnator.config import ValidationConfig
from validnator.pipeline import Pipeline

parquet_path = Path("data/ylt/verisk/example.parquet")
config_path = Path("data/ylt/validnator-verisk.yml")

pipeline = Pipeline.from_config(
    ValidationConfig(
        input_file=parquet_path,
        output_dir=Path("validation-output/verisk-ylt"),
        pipeline_config_file=config_path,
    )
)
df = pl.read_parquet(parquet_path)
results = pipeline.run_with_df(df)
```

Use `data/ylt/validnator-verisk.yml` for Verisk YLT DataFrames and
`data/ylt/validnator-risklink.yml` for RiskLink YLT DataFrames. Both configs are
input-format neutral schema rules for the parquet columns rollup consumes.
