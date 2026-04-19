# RH — pending data exports

**This file has been superseded.** The canonical reference is
[`docs/data-requirements.md`](docs/data-requirements.md), which covers:

- The full file layout the pipeline expects (YLTs, seeds, EP summaries, output).
- The wire schema for every YLT parquet column the pipeline reads.
- For each of the 10 seeds: schema, source, copy-pasteable duckdb `COPY`
  statement (where applicable), and what happens if the seed is empty.
- The currency-derivation rule (substring match on `cds_cat_class_name`).
- The forecast-factor join contract (office string must match exactly).
- A failure-mode reference table mapping symptoms to causes to fixes.

The "OPTIMAL" peril-restructure seeds (`perils.csv`, `analyses.csv`,
`rollup_scope.csv`, `blending_weights.csv`) and the unused legacy seeds
(`cds_region_peril.csv`, `flood_rl22_model_events.csv`) have been removed
from the active pipeline. See `seeds/README.md` for the rationale.
