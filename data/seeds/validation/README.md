# data/seeds/validation

These parquet catalogues support event-day validation and enrichment in rollup:

- `verisk_events.parquet`
- `risklink_flood22_model_events.parquet`

The colocated validnator YAMLs describe the parquet-loaded DataFrame schemas:

- `validnator-verisk-events.yml` checks Verisk event IDs, model IDs, years, and
  event days.
- `validnator-risklink-flood-events.yml` checks RiskLink model event IDs,
  region-peril IDs, and `ModelOccurrenceDate`. The current parquet stores
  `ModelOccurrenceDate` as a Polars datetime; the rule requires values castable
  to `date`, matching the event-day derivation use case.

These configs intentionally omit an `input:` block because they validate typed
DataFrames supplied programmatically. `validnator-verisk-events.yml` uses strict
schema matching for the current parquet integer columns. RiskLink flood keeps a
castable schema check only for `ModelOccurrenceDate` because the parquet stores a
Polars datetime and rollup derives a date from it.

Validate these parquet catalogues by loading them into Polars and using
`Pipeline.run_with_df(df)`:

```python
from pathlib import Path

import polars as pl
from validnator.config import ValidationConfig
from validnator.pipeline import Pipeline

parquet_path = Path("data/seeds/validation/verisk_events.parquet")
config_path = Path("data/seeds/validation/validnator-verisk-events.yml")

pipeline = Pipeline.from_config(
    ValidationConfig(
        input_file=parquet_path,
        output_dir=Path("validation-output/verisk-events"),
        pipeline_config_file=config_path,
    )
)
df = pl.read_parquet(parquet_path)
results = pipeline.run_with_df(df)
```

If a CSV extract needs validation, use a separate CSV config with `input: {type:
csv, mode: raw_strings}` so validnator reads raw strings and checks castability.
