# Rollup Pipeline

The rollup pipeline converts analyst-supplied catastrophe inputs into Hiscox mart
parquets and analysis reports.

## Command summary

Use `uv run rollup ...` from a fresh checkout. After installing the project and
activating the virtual environment, use `rollup ...` directly.

```bash
uv run rollup validate        # check inputs before a run
uv run rollup run             # write normal outputs, DuckDB, and analysis/ep_report.csv
uv run rollup run --no-duckdb # skip the default DuckDB export
uv run rollup run --debug     # also write intermediate frames to output/debug/
uv run rollup docs            # serve these docs locally
uv run rollup docs --host localhost --port 4322
```

## Inputs and outputs

- Analyst inputs live under `data/`.
- Generated outputs default to root `output/`.
- Mart fanouts are written to `output/marts/`.
- Wide/report parquets are written to `output/`.
- DuckDB export is enabled by default; pass `--no-duckdb` to disable it.
- Final event rows below `minimum_event_loss_threshold` are excluded from final
  marts. `mts_tbl_ylt_dialsup.parquet` contains only final
  `dialsup_gbp_forecast` rows.
- `output/mts_tbl_ylt_combined_all_factors_wide.parquet` includes forecast loss
  columns such as `euws_override_YYYYMM_loss` and
  `dialsup_gbp_forecast_YYYYMM_loss`.
- `output/analysis/ep_report.csv` is written by `rollup run` and can be
  regenerated with `rollup analyze`.
- Debug frames are written to `output/debug/` only when `--debug` is used.

Start with [Quickstart](first-run.md). On Windows, use the
[Windows install guide](windows-install.md) first to install `uv` and build a
local environment. Then use [Loading your data](load-data.md) for exact file
locations and [EP summaries](ep-summaries.md) when converting wide vendor CSVs
to `.long.csv` inputs. If a YLT arrives as CSV, see [Utilities](utilities.md)
for the DuckDB CSV-to-Parquet command. For reference details, see the
[data-flow architecture](architecture.md), [calculation reference](calculation-reference.md),
and [seed files](data-requirements.md#seed-files).
