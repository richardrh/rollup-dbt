# Programmatic API

Use the API from Dataiku recipes, notebooks, or Python scripts. The API is the
stable integration surface; the CLI is a thin local wrapper.

## `validate_rollup_inputs`

```python
from rollup.api import validate_rollup_inputs

validation = validate_rollup_inputs("data")
if not validation.is_valid:
    print(validation.validation_report)
```

`validate_rollup_inputs(data_root)` validates source availability and
schema/nullability for the main inputs. Expected source/schema failures return
`RollupValidationResult(is_valid=False)` with a Polars validation report.
Unexpected errors propagate.

Call `validation.raise_for_errors()` when you want invalid inputs to raise
`RollupValidationError`.

## `run_rollup`

```python
from rollup.api import run_rollup

result = run_rollup(
    data_root="data",
    output_root="output",
    validate=True,
    write_analysis=True,
    log_file="output/rollup.log",
)
```

Parameters commonly used by callers:

| Parameter | Default | Meaning |
| --- | --- | --- |
| `data_root` | `"data"` | Input root. |
| `output_root` | `"output"` | Output root. |
| `config_path` | `None` | Optional TOML path; defaults to `rollup.local.toml` when no config object is passed. |
| `config` | `None` | Optional `RollupConfig` object. Takes precedence over `config_path`. |
| `validate` | `True` | Raise on expected validation failures before calculation. |
| `write_analysis` | `True` | Write `analysis/ep_report.csv`. |
| `validation_callback` | `None` | Optional callback receiving the validation result. |
| `log_file` | `None` | Optional log path. CLI supplies `<output-root>/rollup.log` by default. |

`run_rollup` returns `RollupRunResult` with `data_root`, `output_root`, output
paths, and optional `ep_report_path`.

## Config object example

```python
from dataclasses import replace
from rollup.api import run_rollup
from rollup.config import load_config

config = load_config("rollup.local.toml")
config = replace(config, outputs=replace(config.outputs, write_duckdb=True))

run_rollup("data", "output", config=config)
```
