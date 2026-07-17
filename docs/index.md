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
uv run rollup generate-ep-summaries --vendor verisk --csv verisk_clean.csv
uv run rollup cleanup --yes   # remove generated outputs without prompting
```

Product CLI commands are `run`, `generate-ep-summaries`, `validate`, and
`cleanup`. Serve repository docs directly with the pinned development docs tool:

```bash
uv run zensical serve --config-file zensical.toml --dev-addr localhost:4322
```

## Inputs and outputs

- Analyst inputs live under `data/`.
- Generated outputs default to root `output/`.
- Mart fanouts are written to `output/marts/`.
- Wide/report parquets are written to `output/`.
- DuckDB export is enabled by default at `output/rollup.duckdb`; pass
  `--no-duckdb` to disable it.
- The DuckDB export includes generated `mts_tbl_*.parquet` tables, mart fanout
  parquets, recursive CSV/parquet seeds including validation catalogue seeds, and
  `output/analysis/ep_report.csv` as `ep_report` when present. It excludes raw
  YLT inputs.
- Final event rows below `minimum_event_loss_threshold` are excluded from final
  marts. `mts_tbl_ylt_dialsup.parquet` contains only final
  `dialsup_localccy_forecast` rows.
- `output/mts_tbl_ylt_combined_all_factors_wide.parquet` includes forecast loss
  columns such as `euws_override_YYYYMM_loss` and
  `dialsup_localccy_forecast_YYYYMM_loss`.
- `output/analysis/ep_report.csv` is written by `rollup run`.
- Debug frames are written to `output/debug/` only when `--debug` is used.

Use the templates in [`sql/`](../sql/) to inspect `output/rollup.duckdb`,
starting with `sql/01_inventory.sql`.

Start with [Quickstart](first-run.md). On Windows, use the
[Windows install guide](windows-install.md) first to install `uv` and build a
local environment. Then use [Loading your data](load-data.md) for exact file
locations and [EP summaries](ep-summaries.md) when converting wide vendor CSVs
to `.long.csv` inputs. If a YLT arrives as CSV, see [Utilities](utilities.md)
for DuckDB CSV-to-Parquet guidance. For reference details, see the
[data-flow architecture](architecture.md), [calculation reference](calculation-reference.md),
and [seed files](data-requirements.md#seed-files).
