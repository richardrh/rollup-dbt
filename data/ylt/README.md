# data/ylt

Rollup reads production YLT inputs as parquet from the vendor subfolders.
The YAML files in this folder describe the expected vendor YLT schema for typed
Polars/Dataiku DataFrames loaded from parquet.

## Parquet validation from Polars/Dataiku DataFrames

Load parquet with Polars (or receive the equivalent Dataiku DataFrame converted
to Polars) and call `Pipeline.run_with_df(df)`. The YAMLs intentionally omit an
`input:` block because the DataFrame is supplied programmatically rather than
loaded by the validnator CSV input loader:

```python
from pathlib import Path

import polars as pl
from validnator.config import ValidationConfig
from validnator.pipeline import Pipeline

parquet_path = Path("data/ylt/verisk/air_ylt_c1.parquet")
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
`data/ylt/validnator-risklink.yml` for RiskLink YLT DataFrames. Both configs use
strict schema matching for the required parquet columns while allowing extra
vendor columns that rollup does not consume.

## CSV extract validation

The local validnator CLI input loader accepts CSV inputs. These parquet/DataFrame
YAMLs do not declare CSV raw-string loading, so only use the CLI with a separate
CSV-oriented config that includes:

```yaml
input:
  type: csv
  mode: raw_strings
```

Example once such a CSV config exists:

```bash
validnator validate -p path/to/verisk-ylt-csv-validnator.yml -i exports/verisk_ylt.csv -o validation-output/verisk-ylt-csv
```
