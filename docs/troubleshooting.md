# Troubleshooting

## Start with validation

```bash
uv run rollup validate
```

Validation checks required input/seed presence and modelled LOB/peril references.
The anti-join report shows values in EP summaries or YLT data that are missing
from `lobs.csv` or `perils.csv` (data-to-seed direction only). Seed rows without
matching data do not appear in the report and are silently ignored downstream.

## Common validation failures

- Missing required source folders, YLT files, or seed tables.
- EP summary `modelled_lob` missing from `data/seeds/business/lobs.csv`.
- EP summary `modelled_peril` missing from `data/seeds/business/perils.csv`.
- Verisk YLT `ExposureAttribute` missing from `lobs.csv`.
- Verisk YLT `Analysis` missing from `perils.csv`.
- Non-empty `Modelled LOB/peril anti-join report`. This is blocking: add/fix the
  value in `lobs.csv`/`perils.csv` or correct the input data.

The `input_ylt_aal_by_lob_peril_summary.csv` report is informational. Review AAL
by vendor, LOB, and peril for obvious issues.

## Empty or surprising outputs

Run with debug output:

```bash
uv run rollup run --debug
```

Inspect likely choke-point files under `output/debug/`, especially canonical
source EP summaries (`src_ep_summaries`), enriched EP summaries
(`int_ep_summaries_enriched`), EP blending targets, YLT blending output, and the
combined YLT debug frame.

## Missing FX or forecast factors

- Missing FX rows are not defaulted. Add the required local-currency-to-GBP rate
  in `data/seeds/vor/fx_rates.csv`; the pipeline inverts this rate to convert
  GBP input losses to local-currency output losses.
- Missing forecast factors default to `1.0`.

## EP report missing

`ep_report.csv` is written by normal `rollup run`. Programmatic callers can
regenerate it from existing outputs with `rollup.analysis.write_ep_report()`.
