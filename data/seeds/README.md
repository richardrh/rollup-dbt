# data/seeds

Seed inputs are described by colocated `validnator*.yml` files. Most seed files
are CSVs; validation catalogues are parquet. CSV validation and parquet/DataFrame
validation use different validnator modes:

- CSV configs include `input: {type: csv, mode: raw_strings}` so validnator reads
  raw strings and checks whether values are castable to the expected types.
- Parquet catalogue configs omit `input:` and validate typed Polars/Dataiku
  DataFrames loaded with `pl.read_parquet(...)` via `Pipeline.run_with_df(df)`.

Configs are colocated with each seed section. Sections containing multiple
schemas use multiple clearly named `validnator-*.yml` files rather than one YAML
that would require unrelated columns in a single file.

Business seeds:

- `business/lobs.csv`: maps source `modelled_lob` values to rollup LOB,
  class, office, currency, and CDS class metadata.
- `business/perils.csv`: maps source `modelled_peril` values to rollup peril,
  region/peril labels, and region-peril IDs.

VOR/adjustment seeds:

- `vor/blending_factors.csv`: blend weights by rollup peril and return-period
  bucket.
- `vor/fx_rates.csv`: currency conversion rates into GBP.
- `vor/forecast_factors.csv`: forecast-date factors by class and office.
- `vor/euws_rate_factors.csv`: Europe Windstorm event-level rate factors.
- `adjustments/euws_rank_overrides.csv`: configured overrides for selected
  zero EUWS factors on top-ranked rows.

Validation catalogues:

- `validation/verisk_events.parquet`: Verisk event/year/model mapping and event
  days.
- `validation/risklink_flood22_model_events.parquet`: RiskLink flood occurrence
  dates used to derive event-day values.

Parquet catalogue DataFrame schema rules live beside those files under
`validation/`:

- `validation/validnator-verisk-events.yml`
- `validation/validnator-risklink-flood-events.yml`

Validnator CSV raw-string CLI examples:

```bash
validnator validate -p data/seeds/business/validnator-lobs.yml -i data/seeds/business/lobs.csv -o validation-output/lobs
validnator validate -p data/seeds/business/validnator-perils.yml -i data/seeds/business/perils.csv -o validation-output/perils
validnator validate -p data/seeds/vor/validnator-fx-rates.yml -i data/seeds/vor/fx_rates.csv -o validation-output/fx-rates
validnator validate -p data/seeds/adjustments/validnator.yml -i data/seeds/adjustments/euws_rank_overrides.csv -o validation-output/euws-rank-overrides
```

For parquet catalogues, load the file and run validnator against the DataFrame:

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

EP summaries under `data/ep_summaries/**/*.long.csv`, including
`data/ep_summaries/risklink/rms_ep_summary.long.csv` and
`data/ep_summaries/verisk/verisk_ep_summary.long.csv`, are the source/version of
truth for what gets modelled. No `selected_analyses.csv` is used. Selected scope
comes from the canonical long EP summaries plus `business/lobs.csv` and
`business/perils.csv`; YLTs under `data/ylt/verisk/*.parquet` and
`data/ylt/risklink/*.parquet` are filtered/enriched from the selected EP summary
variants.
