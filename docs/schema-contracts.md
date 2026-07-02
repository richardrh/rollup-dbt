# Schema contracts

`schema.yaml` files are the pipeline's data contracts. They are colocated with
the data area they describe and act as the reference point for required files,
columns, and types.

| Contract file | Describes |
| --- | --- |
| `data/seeds/schema.yaml` | Seed CSV/parquet lookup and validation files |
| `data/ylt/schema.yaml` | Vendor YLT parquet inputs |
| `data/ep_summaries/schema.yaml` | Canonical long EP summary CSV inputs |

## Contract shape

Each `schema.yaml` contains a `datasets` map. Each dataset entry names one file
or file group and defines its expected structure:

| Field | Meaning |
| --- | --- |
| Dataset name | Stable key for the dataset, such as `lobs` or `raw_verisk_ylt`. |
| `role` | Dataset purpose, for example `source` or `mart`. |
| `path` / `glob` | Exact file path or glob pattern to scan. |
| `format` | File format, usually `csv` or `parquet`. |
| `required` | Whether validation should expect the dataset to be present. |
| `description` | Human-readable purpose of the dataset. |
| `columns` | Ordered list of expected columns. |
| `allow_extra_columns` | Optional dataset policy. Defaults to `false`; raw vendor YLT datasets set it to `true`. |

Each column entry defines:

| Column field | Meaning |
| --- | --- |
| `name` | Required column name. |
| `dtype` | Expected logical type, such as `string`, `int64`, or `float64`. |
| `required` | Whether the column must be present. |
| `description` | Business meaning of the column. |

## How validation uses the contracts

`uv run rollup validate` reads the colocated schema files before deeper data
checks. The schema files are the source of truth for:

- seed CSV filenames and columns discovered from `data/seeds/schema.yaml`;
- YLT parquet globs and columns from `data/ylt/schema.yaml`;
- EP summary long CSV globs and columns from `data/ep_summaries/schema.yaml`;
- expected formats, required flags, dtypes, roles, and descriptions.

The validation report flags contract failures such as:

- missing scanned input areas or required files;
- seed files with no matching schema;
- missing required columns;
- unexpected columns when the dataset is strict;
- dtype mismatches for required columns and for optional columns when present.

Raw vendor YLT contracts are intentionally minimal and allow extra export
columns. Their globs are `data/ylt/verisk/*.parquet` and
`data/ylt/risklink/*.parquet`: validation expects at least one direct matching
parquet file per vendor folder, validates each direct child parquet, and the
loader scans all direct matches. There is no required filename convention beyond
`.parquet` in the correct vendor folder. Subdirectories are ignored, and inactive
or test parquet files should not be left in active vendor folders because they
will be loaded. Seed CSVs and canonical EP summary CSVs remain strict by default.
Verisk YLT file names are derived from parquet paths for validation reporting, so
a row-level `filename` column is optional rather than required. RiskLink YLT only
requires `anlsid`, `yearid`, `eventid`, and `loss`; `anlsid` must match RiskLink
EP summary `analysis_id` values.

Fix schema-validation failures before investigating modelled LOB/peril failures.
After file-level schemas pass, validation runs anti-join checks against
`data/seeds/business/lobs.csv` and `data/seeds/business/perils.csv`, then prints a
YLT loss validation summary and raw input YLT AAL by LOB/peril summary as sanity
checks. Add `--report-dir output/validation` to write the same validation tables
to CSV files.

## Why this is the anchor point

Use `schema.yaml` as the first place to answer "what shape should this dataset
have?"

- Analysts use it to confirm required files, columns, types, and business
  descriptions.
- Developers update it whenever they add or change an input, stage, output, or
  mart contract.
- Reviewers use it to see whether code, tests, and documentation agree on the
  pipeline's expected data shape.

Keep contracts close to the data area they describe. If a pipeline change adds a
new input/output contract or changes an existing one, update the appropriate
`schema.yaml`, validation tests, and user-facing docs in the same change.
