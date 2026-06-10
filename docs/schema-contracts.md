# Validnator Contracts

Colocated `validnator*.yml` files are the reference data contracts for remote
callers and external validation workflows. They live beside the data area they
describe and document required files, columns, and types.

The runtime does **not** load these YAML files. Runtime protection is implemented
with hard-coded Pandera schemas in `src/rollup/staging/load_sources.py` so the
pipeline fails before calculations when required inputs have the wrong shape.

| Contract file | Describes |
| --- | --- |
| `data/ep_summaries/validnator.yml` | Canonical long EP summary CSV inputs |
| `data/seeds/business/validnator-lobs.yml` | Business LOB seed CSV |
| `data/seeds/business/validnator-perils.yml` | Business peril seed CSV |
| `data/seeds/vor/validnator-*.yml` | VOR seed CSVs |
| `data/seeds/adjustments/validnator.yml` | EUWS rank override seed CSV |
| `data/seeds/validation/validnator-*.yml` | Validation parquet catalogues supplied as DataFrames |
| `data/ylt/validnator-*.yml` | Vendor YLT parquet inputs supplied as DataFrames |

## Contract shape

Each Validnator config defines the input mode and validation rules for one data
file or file family:

| Field | Meaning |
| --- | --- |
| `input` | Input loader configuration. CSV contracts use raw-string input; parquet/DataFrame contracts omit CLI input. |
| `rules` | Ordered Validnator rules for schema, null, range, and value checks. |
| `schema_matches` | Column names, logical dtypes, and extra-column policy. |
| `allow_extra_columns` | Dataset policy. Runtime CSV inputs are strict; raw YLT parquet/DataFrame inputs allow extra export columns. |

Each column entry defines:

| Column field | Meaning |
| --- | --- |
| `name` | Required column name. |
| `dtype` | Expected logical type, such as `string`, `int64`, or `float64`. |
| `required` | Whether the column must be present. |
| `description` | Business meaning of the column. |

## How runtime validation works

`validate_rollup_inputs("data")` calls the runtime source loader and validates the
inputs consumed by the pipeline. It checks:

- at least one direct YLT parquet file under each vendor folder;
- required YLT columns and required-column nulls;
- strict EP summary long CSV columns;
- strict seed CSV columns for business, VOR, and adjustment seeds;
- optional validation/event catalogues when their folders are present.

The validation report flags runtime guard failures such as:

- missing scanned input areas or required files;
- missing required columns;
- unexpected columns in strict EP/seed CSVs;
- dtype mismatches or uncastable required columns;
- nulls in required runtime columns.

Raw vendor YLT contracts are intentionally minimal and allow extra export
columns. Their globs are `data/ylt/verisk/*.parquet` and
`data/ylt/risklink/*.parquet`: validation expects at least one direct matching
parquet file per vendor folder, validates each direct child parquet, and the
loader scans all direct matches. There is no required filename convention beyond
`.parquet` in the correct vendor folder. Subdirectories are ignored, and inactive
or test parquet files should not be left in active vendor folders because they
will be loaded. Seed CSVs and canonical EP summary CSVs are strict at runtime.
Verisk YLT file names are derived from parquet paths for validation reporting, so
a row-level `filename` column is optional rather than required. RiskLink YLT only
requires `anlsid`, `yearid`, `eventid`, and `loss`; `anlsid` must match RiskLink
EP summary `analysis_id` values.

Fix runtime schema-validation failures before investigating calculation outputs.
Use a no-output-analysis smoke run when validating a full data drop locally:

```bash
uv run python -m rollup run --data-root data --output-root output --target-currency GBP --no-stage-outputs --no-analysis
```

## Why this is the anchor point

Use the colocated Validnator configs as documentation for external callers asking
"what shape should this dataset have?"

- Analysts use them to confirm required files, columns, types, and business
  descriptions.
- Developers update them whenever they add or change an input, stage, output, or
  mart contract.
- Reviewers use it to see whether code, tests, and documentation agree on the
  pipeline's expected data shape.

Keep contracts close to the data area they describe. If a pipeline change adds a
new input/output contract or changes an existing one, update the appropriate
Validnator YAML, runtime Pandera schema, and user-facing docs in the same change.
