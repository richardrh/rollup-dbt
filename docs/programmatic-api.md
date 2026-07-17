# Programmatic API

Use `rollup.api` when calling the pipeline from Python tools such as Dataiku.
Do not shell out to `rollup run` unless you specifically need terminal-style
output.

The supported public run API is `run_rollup` from `rollup.api`, returning a flat
`RollupRunResult` dataclass. EP-summary conversion lives in
`rollup.ep_summary_generator`; call that module directly.
If you need to read TOML configuration directly, use
`rollup.config.read_config(...)`.

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
print(result.mts_wide)
print(result.mart_files)
```

`run_rollup` runs the pipeline, writes normal outputs, and
writes `output/analysis/ep_report.csv` by default. If `log_file` is provided,
the API writes run logs for that call and then removes/closes its temporary file
handler so the host application or Dataiku logging setup is not polluted.

## Dataiku recipe: local managed folders

If the Dataiku managed folders expose local filesystem paths with `get_path()`,
call the API directly on those paths. No temporary directory is needed.

```python
from pathlib import Path

import dataiku

from rollup.api import run_rollup


input_folder = dataiku.Folder("ROLLUP_INPUTS")
output_folder = dataiku.Folder("ROLLUP_OUTPUTS")

data_root = Path(input_folder.get_path()) / "data"
output_root = Path(output_folder.get_path()) / "output"

result = run_rollup(
    data_root=data_root,
    output_root=output_root,
    debug=False,
)

print("EP report:", result.ep_report_path)
print("MTS wide:", result.mts_wide)
print("Mart files:", result.mart_files)
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

This is needed because the pipeline discovers directories and creates lazy
parquet/CSV scans through normal filesystem paths.

```python
from pathlib import Path
import shutil
import tempfile

import dataiku

from rollup.api import run_rollup


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

    result = run_rollup(data_root=data_root, output_root=output_root)

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
path for local CLI runs, installed-package runs, and Dataiku runs.

## Validate inputs only

There is no public Python validation helper. Use the CLI validation contract when
you need a validation-only step:

```bash
uv run rollup validate --data-root /path/to/data --report-dir /path/to/output/validation
```

Expected validation input failures return a non-zero exit code with concise
stderr. When reports can be computed, the CLI writes only:

- `modelled_lob_peril_anti_join_report.csv`
- `input_ylt_aal_by_lob_peril_summary.csv`

Programmatic callers should handle normal Python exceptions from `run_rollup`.

## Generate one EP summary long CSV

```python
from rollup.ep_summary_generator import generate_vendor_ep_summary

output_path = generate_vendor_ep_summary(
    data_root="/path/to/data",
    vendor="verisk",
    csv_path="/path/to/data/ep_summaries/verisk/source_wide.csv",
)

print(output_path)
```

This is the non-interactive equivalent of `rollup generate-ep-summaries`.

For API callers that want only the converted frame, use
`rollup.ep_summary_generator.build_ep_summary_from_wide_csv(...)`. For bulk
generation, use `rollup.ep_summary_generator.generate_ep_summaries(...)` only
when each vendor has exactly one candidate source wide CSV. Zero or multiple
candidates for a vendor fail. Use `scan_ep_summary_csvs(...)` to list candidates,
then call `generate_vendor_ep_summary(...)` when you need to select explicitly.

## Rebuild only the EP report

```python
from rollup.analysis import write_ep_report

ep_report_path = write_ep_report("/path/to/output")
```

Use this when pipeline outputs already exist and only
`output/analysis/ep_report.csv` needs to be regenerated.

## Result objects

`run_rollup` returns a `RollupRunResult`:

| Attribute | Description |
| --- | --- |
| `data_root` | Input data root used by the run |
| `output_root` | Output root used by the run |
| `mts_combined` | `output/mts_tbl_ylt_combined_all_factors.parquet` |
| `mts_wide` | `output/mts_tbl_ylt_combined_all_factors_wide.parquet` |
| `mts_dialsup` | `output/mts_tbl_ylt_dialsup.parquet` |
| `event_validation` | `output/mts_event_validation.parquet` |
| `marts_dir` | `output/marts/` |
| `mart_files` | Tuple of generated mart parquet files |
| `debug_dir` | `output/debug/` when `debug=True`, otherwise `None` |
| `duckdb_file` | DuckDB export path when enabled, otherwise `None` |
| `ep_report_path` | Path to `output/analysis/ep_report.csv`, or `None` if disabled |
