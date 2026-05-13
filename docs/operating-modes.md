# Operating mode — how the pipeline decides which analyses to run

The pipeline uses an explicit seed allow-list:

```text
data/seeds/business/valid_analyses.csv
```

## Current model

| Step | What | File / stage | Purpose |
|---|---|---|---|
| 1 | Load raw YLTs | `data/ylt/{vendor}/*.parquet` | Vendor event losses |
| 2 | Load analysis metadata | `analyses.csv` | Vendor analysis ID → peril, plus RiskLink LOB |
| 3 | Filter to valid IDs | `valid_analyses.csv` | Operator-approved numeric vendor analysis IDs |
| 4 | Normalize | `stages/staging.py` | Attach rollup LOB/peril metadata |
| 5 | Validate | `validate_one_peril_per_rollup_lob` | Fail if one rollup LOB maps to multiple perils |

## `valid_analyses.csv`

```csv
vendor,analysis_id
verisk,900002
risklink,29
```

`analysis_id` is the vendor-native numeric analysis ID, stored as text:

- Verisk: numeric analysis ID; raw `Analysis` labels live in `analyses.modelled_label`.
- RiskLink: stringified raw `anlsid` / EP-summary `ID`.

The bundled Verisk IDs are placeholders and must be replaced with real Verisk
analysis IDs before production.

Only listed IDs can contribute YLT rows or EP-summary rows. Peril and LOB are
not repeated in this table; they come from `analyses.csv` and `lobs.csv`.

## Why this replaced the old scope table

The old `rollup_scope` model selected `(modelled_lob, vendor, analysis)`
triples and mimicked January's per-LOB `applies_to_*` flags. The current model
is simpler: operators approve vendor analysis IDs directly, then the pipeline
uses lookup tables to derive rollup peril and rollup LOB.

The invariant is: **each rollup LOB must map to exactly one peril**. The
pipeline validates this after staging and aborts if the data violates it.
