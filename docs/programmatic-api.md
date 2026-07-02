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
    log_file="/path/to/output/run.log",
)

print(result.ep_report_path)
print(result.outputs.mts_wide)
print(result.outputs.mart_files)
```

`run_rollup` validates inputs, runs the pipeline, writes normal outputs, and
writes `output/analysis/ep_report.csv` by default. If `log_file` is provided,
the API writes run logs for that call and then removes/closes its temporary file
handler so the host application or Dataiku logging setup is not polluted.

## Dataiku recipe: local managed folders

If the Dataiku managed folders expose local filesystem paths with `get_path()`,
call the API directly on those paths. No temporary directory is needed.

```python
from pathlib import Path

import dataiku

from rollup.api import RollupValidationError, run_rollup


input_folder = dataiku.Folder("ROLLUP_INPUTS")
output_folder = dataiku.Folder("ROLLUP_OUTPUTS")

data_root = Path(input_folder.get_path()) / "data"
output_root = Path(output_folder.get_path()) / "output"

try:
    result = run_rollup(
        data_root=data_root,
        output_root=output_root,
        debug=False,
    )
except RollupValidationError as exc:
    validation_dir = output_root / "validation"
    exc.validation.write_reports(validation_dir)
    raise

print("EP report:", result.ep_report_path)
print("MTS wide:", result.outputs.mts_wide)
print("Mart files:", result.outputs.mart_files)
```

Expected input folder layout:

```text
ROLLUP_INPUTS/
└── data/
    ├── ep_summaries/
    ├── seeds/
    └── ylt/
```

The API writes outputs under the output managed folder:

```text
ROLLUP_OUTPUTS/
└── output/
    ├── analysis/
    ├── marts/
    ├── mts_tbl_ylt_combined_all_factors.parquet
    ├── mts_tbl_ylt_combined_all_factors_wide.parquet
    ├── mts_tbl_ylt_dialsup.parquet
    └── mts_event_validation.parquet
```

## Dataiku recipe: remote managed folders

If the managed folder does **not** expose a local filesystem path, stage files to
a `TemporaryDirectory`, run the API locally, then upload generated outputs back
to the output managed folder.

This is needed because the pipeline scans directories and parquet files through
normal filesystem paths.

```python
from pathlib import Path
import shutil
import tempfile

import dataiku

from rollup.api import RollupValidationError, run_rollup


def download_managed_folder(folder, local_root: Path) -> None:
    for remote_path in folder.list_paths_in_partition():
        if remote_path.endswith("/"):
            continue
        local_path = local_root / remote_path.lstrip("/")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with folder.get_download_stream(remote_path) as source:
            with local_path.open("wb") as target:
                shutil.copyfileobj(source, target)


def upload_tree(local_root: Path, folder) -> None:
    for local_path in local_root.rglob("*"):
        if not local_path.is_file():
            continue
        remote_path = "/" + local_path.relative_to(local_root).as_posix()
        with local_path.open("rb") as source:
            folder.upload_stream(remote_path, source)


input_folder = dataiku.Folder("ROLLUP_INPUTS")
output_folder = dataiku.Folder("ROLLUP_OUTPUTS")

with tempfile.TemporaryDirectory(prefix="rollup-dataiku-") as tmp_dir:
    work_root = Path(tmp_dir)
    local_input_root = work_root / "input"
    local_output_root = work_root / "output"

    download_managed_folder(input_folder, local_input_root)

    data_root = local_input_root / "data"
    output_root = local_output_root / "output"

    try:
        result = run_rollup(data_root=data_root, output_root=output_root)
    except RollupValidationError as exc:
        validation_dir = local_output_root / "validation"
        exc.validation.write_reports(validation_dir)
        upload_tree(local_output_root, output_folder)
        raise

    upload_tree(local_output_root, output_folder)

print("EP report:", result.ep_report_path)
```

Use the temporary-directory recipe for S3, Azure Blob, shared object storage, or
any managed folder where `get_path()` is unavailable or not a real local path.

## Do we need a temp directory?

Use this rule:

| Dataiku storage type | Temp directory needed? | Why |
| --- | --- | --- |
| Filesystem managed folder with `get_path()` | No | The API can read/write real local paths directly |
| Remote/object-store managed folder | Yes | Stage to local disk, run, upload outputs back |

The pipeline API is intentionally filesystem-based. That keeps the same code
path for local runs, PyInstaller bundle runs, CLI runs, and Dataiku runs.

## Validate inputs only

```python
from rollup.api import validate_rollup_inputs

validation = validate_rollup_inputs(
    "/path/to/data",
    report_dir="/path/to/output/validation",  # optional
)

if not validation.is_valid:
    validation.raise_for_errors()
```

The validation object contains Polars DataFrames:

- `validation_report`
- `coverage_report`
- `ylt_loss_report`
- `input_ylt_aal_report`

You can write all validation frames to CSV with:

```python
written_paths = validation.write_reports("/path/to/output/validation")
```

The files are:

- `validation_report.csv`
- `modelled_lob_peril_anti_join_report.csv`
- `ylt_loss_validation_summary.csv`
- `input_ylt_aal_by_lob_peril_summary.csv`

## Handle validation failures

```python
from rollup.api import RollupValidationError, run_rollup

try:
    result = run_rollup("/path/to/data", "/path/to/output")
except RollupValidationError as exc:
    exc.validation.write_reports("/path/to/output/validation")
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
