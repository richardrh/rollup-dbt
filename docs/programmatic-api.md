# Programmatic API

Use the API from Dataiku recipes, notebooks, or Python scripts. The API is the
stable integration surface; the CLI is a thin local wrapper.

## Validation boundary

Validnator is the source of truth for input/schema validation. Run Validnator on
the documented data contracts before calling the runtime. The runtime does not
expose a separate schema validation API; it keeps business invariants that can
only be checked after computation or joins, and lets missing files needed for
execution fail normally.

## `run_rollup`

```python
from pathlib import Path
from rollup.api import run_rollup

result = run_rollup(
    data_root=Path("data"),
    output_root=Path("output"),
    config_path=Path("config.toml"),
    write_analysis=True,
    log_file="output/rollup.log",
)
```

Parameters commonly used by callers:

| Parameter | Default | Meaning |
| --- | --- | --- |
| `data_root` | `"data"` | Input root. |
| `output_root` | `"output"` | Output root. |
| `config_path` | `None` | Optional TOML path; defaults to tracked `config.toml` when no config object is passed. |
| `config` | `None` | Optional `RollupConfig` object. Takes precedence over `config_path`. |
| `write_analysis` | `True` | Write `analysis/ep_report.csv`. |
| `log_file` | `None` | Optional log path. CLI supplies `<output-root>/rollup.log` by default. |

`run_rollup` returns `RollupRunResult` with `data_root`, `output_root`, output
paths, and optional `ep_report_path`.

For Dataiku callers, pass `config_path` explicitly for job-specific configs. Do
not rely on the current working directory default. The repository default is the
tracked `config.toml`; Dataiku jobs may copy or write their own `config.toml` in
a job workspace and pass that path explicitly.

## Config loading

`run_rollup(...)` loads config like this:

1. If `config` is supplied, use it and ignore `config_path`.
2. Otherwise, load the TOML at `config_path`.
3. If neither is supplied, try tracked `config.toml` in the current working
   directory.
4. If no TOML exists, use the dataclass defaults in `rollup.config`.

`rollup.local.toml` is not loaded automatically; pass it via `config_path` when
you want a local override.

The important TOML sections are:

```toml
[fx]
target_currency = "GBP"

[outputs]
write_stage_outputs = true
write_duckdb = false
duckdb_file = "rollup.duckdb"

[outputs.fanout_prefixes]
verisk = "HiscoAIR"
risklink = "HiscoRMS"

[analysis]
return_periods = [30, 200, 1000]

[vendor_years]
verisk = 10000
risklink = 100000

[blending]
uplift_factor_min = 0.1
uplift_factor_max = 10.0
target_points = [
    { ep_type = "AAL", return_period = 0 },
    { ep_type = "OEP", return_period = 200 },
    { ep_type = "OEP", return_period = 1000 },
]
```

Vendor years control EP report rank/AAL calculations and the YLT
rank-to-return-period bucket conversion used by EP-derived blending. Blend
target points, uplift clipping bounds, and fanout filename prefixes are config
values. VOR subregion choices are business mappings in `perils.csv` through
`blend_subregion_peril_id`.

The `"216" = "216b"` subregion selection means: when VOR
`blending_factors.csv` has multiple factor rows for Europe Flood
`RegionPerilID` `216` (`216a`, `216b`, `216c`), use the `216b` row. It does not
change the base model. The base model comes from `perils.csv`; for Europe Flood,
modelled flood perils map to `Europe_FL` with `base_model=risklink`, so the
uplift is applied to the RiskLink YLT. In the current seed, `216b` is Germany
Flood and supplies the AIR/RMS blend weights from that row.

## Dataiku workspace pattern

The path-based API works well when Dataiku can expose managed folders as local
paths. Prefer passing that folder path directly as `data_root` when it already
matches the expected input layout.

When Dataiku inputs need to be materialized first, create a job workspace and
pass all three paths explicitly:

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from rollup.api import run_rollup

with TemporaryDirectory() as workspace:
    workspace = Path(workspace)
    data_root = workspace / "data"
    output_root = workspace / "output"
    config_path = workspace / "config.toml"

    # Materialize/copy Dataiku inputs into data_root using the documented layout.
    # Write the job-specific TOML to config_path.

    result = run_rollup(
        data_root=data_root,
        output_root=output_root,
        config_path=config_path,
        write_analysis=False,
        log_file=output_root / "rollup.log",
    )

    # Persist the files referenced by result.outputs before the temp dir exits.
```

This is a local filesystem workspace. Exact Dataiku managed-folder and temp
paths vary by project, so treat the paths above as examples. Avoid copying
`data_root` when Dataiku already provides a usable managed-folder path; use the
temporary workspace for config and outputs only. Persist files referenced by
`result.outputs` to the appropriate managed folder or dataset.

## Returned outputs

`run_rollup(...)` returns a `RollupRunResult`:

```python
result.data_root
result.output_root
result.ep_report_path

result.outputs.mts_combined
result.outputs.mts_wide
result.outputs.mts_dialsup
result.outputs.marts_dir
result.outputs.mart_files
result.outputs.stage_dir
result.outputs.duckdb_file
```

`result.outputs.mart_files` contains all parquet files currently present in the
mart directory, including configured fanout files.

`result.outputs.stage_dir` is `None` when `[outputs].write_stage_outputs =
false`. `result.outputs.duckdb_file` is `None` when `[outputs].write_duckdb =
false`. `result.ep_report_path` is `None` when `write_analysis=False`.

## DuckDB output

DuckDB export is controlled by config. Enable it in the TOML passed through
`config_path`:

```toml
[outputs]
write_duckdb = true
duckdb_file = "rollup.duckdb"
```

```python
result = run_rollup(
    data_root=Path("data"),
    output_root=Path("output"),
    config_path=Path("config.toml"),
)
```

With the relative path above, the file is written to
`<output_root>/rollup.duckdb` and returned as `result.outputs.duckdb_file`.
Use an absolute `duckdb_file` to write outside `output_root`.

The DuckDB export includes `mts_tbl_ylt_combined_all_factors` and
`mts_tbl_ylt_dialsup`.

To disable DuckDB:

```toml
[outputs]
write_duckdb = false
```

Then `result.outputs.duckdb_file is None`.

## Config object example

```python
from dataclasses import replace
from rollup.api import run_rollup
from rollup.config import load_config

config = load_config("config.toml")
config = replace(config, outputs=replace(config.outputs, write_duckdb=True))

run_rollup("data", "output", config=config)
```

## EP summary conversion

```python
from rollup.api import convert_ep_summary

frame = convert_ep_summary(
    input_csv="data/ep_summaries/verisk/verisk_clean.csv",
    vendor="verisk",
)

frame = convert_ep_summary(
    input_csv="data/ep_summaries/verisk/verisk_clean.csv",
    vendor="verisk",
    output_csv="data/ep_summaries/verisk/verisk_ep_summary.long.csv",
)
```

`convert_ep_summary(...)` converts one wide CSV to the canonical long shape and
returns a Polars `DataFrame`. Pass `output_csv` only when you also want the
converted rows written to disk.

For local operator workflows, `convert_ep_summaries("data")` scans the configured
vendor folders, ignores existing `.long.csv` files, writes one output per vendor,
and returns the output paths.
