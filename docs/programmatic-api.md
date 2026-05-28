# Programmatic API

Use `rollup.api` when calling the pipeline from Python tools such as Dataiku.
Do not shell out to `rollup run` unless you specifically need terminal-style
output.

## Run the full pipeline

```python
from rollup.api import run_rollup

result = run_rollup(
    data_root="/path/to/data",
    output_root="/path/to/output",
    debug=False,
)

print(result.ep_report_path)
print(result.outputs.mts_wide)
print(result.outputs.mart_files)
```

`run_rollup` validates inputs, runs the pipeline, writes normal outputs, and
writes `output/analysis/ep_report.csv` by default.

## Validate inputs only

```python
from rollup.api import validate_rollup_inputs

validation = validate_rollup_inputs("/path/to/data")

if not validation.is_valid:
    validation.validation_report.write_csv("validation_report.csv")
    validation.coverage_report.write_csv("lob_peril_coverage.csv")
    validation.raise_for_errors()
```

The validation object contains Polars DataFrames:

- `validation_report`
- `coverage_report`
- `ylt_loss_report`
- `input_ylt_aal_report`

## Handle validation failures

```python
from rollup.api import RollupValidationError, run_rollup

try:
    result = run_rollup("/path/to/data", "/path/to/output")
except RollupValidationError as exc:
    exc.validation.validation_report.write_csv("failed_validation.csv")
    exc.validation.coverage_report.write_csv("failed_lob_peril_coverage.csv")
    raise
```

The API raises Python exceptions instead of returning CLI exit codes.

## Generate one EP summary long CSV

```python
from rollup.api import generate_ep_summary

output_path = generate_ep_summary(
    data_root="/path/to/data",
    vendor="verisk",
    csv_path="/path/to/data/ep_summaries/verisk/source_wide.csv",
)

print(output_path)
```

This is the non-interactive equivalent of `rollup generate-ep-summaries`.

## Rebuild only the EP report

```python
from rollup.api import build_ep_report

ep_report_path = build_ep_report("/path/to/output")
```

Use this when pipeline outputs already exist and only
`output/analysis/ep_report.csv` needs to be regenerated.

## Result objects

`run_rollup` returns a `RollupRunResult`:

| Attribute | Description |
| --- | --- |
| `data_root` | Input data root used by the run |
| `output_root` | Output root used by the run |
| `validation` | Structured validation result |
| `outputs` | Generated output paths |
| `ep_report_path` | Path to `output/analysis/ep_report.csv`, or `None` if disabled |

`result.outputs` is a `RollupOutputPaths` object:

| Attribute | Path |
| --- | --- |
| `mts_combined` | `output/mts_tbl_ylt_combined_all_factors.parquet` |
| `mts_wide` | `output/mts_tbl_ylt_combined_all_factors_wide.parquet` |
| `mts_dialsup` | `output/mts_tbl_ylt_dialsup.parquet` |
| `event_validation` | `output/mts_event_validation.parquet` |
| `marts_dir` | `output/marts/` |
| `mart_files` | Tuple of generated mart parquet files |
| `debug_dir` | `output/debug/` when `debug=True`, otherwise `None` |
