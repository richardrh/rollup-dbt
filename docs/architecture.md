# Architecture

How the code is organised, and why. For the high-level overview + run
commands see [`../polars/README.md`](../polars/README.md); for the factor chain see
[`factor-chain.md`](factor-chain.md).

## Why `polars/` (folder) vs `rollup/` (package)

A Python package named `polars` at this repo root would shadow `import polars`
(the library). So the on-disk folder is `polars/` but the importable package
inside is `rollup/`. All internal imports read `from rollup.X import Y`.
`conftest.py` puts this folder on `sys.path`.

## Vendors — one source of truth

```python
from rollup import config
from rollup.config import VendorName

cfg = config.resolve()
verisk   = cfg.vendor(VendorName.VERISK)    # Vendor(hisco_label='AIR', n_simulations=10_000,  ylt_dir, ...)
risklink = cfg.vendor(VendorName.RISKLINK)  # Vendor(hisco_label='RMS', n_simulations=100_000, ylt_dir, ...)
```

- `n_simulations` drives the return-period math in `ep_curve_from_ylt`.
- `hisco_label` ("AIR" / "RMS") is the external contract — it appears in
  output file names `HiscoAIR_*.parquet` / `HiscoRMS_*.parquet`.
- Vendors declare their `flavors` tuple; default is `(MAIN, DIALSUP)`.

## Flavors

Two Hisco output flavours:

- `Flavor.MAIN` — the main output: capped, local-ccy, forecast-adjusted,
  euws-adjusted. Backed by the
  `loss_uplifted_capped_localccy_{tag}_euws` column.
- `Flavor.DIALSUP` — sensitivity output: `loss × forecast × EUWS`.
  Single column, using the selected forecast tag rather than emitting one
  file per forecast date.
  One file per vendor.

## VariantSpec — what gets fanned out

```python
class VariantSpec(NamedTuple):
    vendor:        Vendor
    forecast_date: date            # comes from forecast_factors seed at runtime
    flavor:        Flavor
```

Variants are built dynamically: `vendor × forecast_date × flavor`. However,
dialsup is emitted once per vendor, using the selected forecast tag, so only
one dialsup variant is created per vendor.

Current setup: 2 vendors × 3 forecast dates (MAIN) + 2 vendors (DIALSUP) → **8 variants**.
Add a forecast date to `forecast_factors.csv` → 10 variants automatically, no code change.

## Typed columns

- `rollup/schemas/columns.py` — one `StrEnum` per logical frame.
- `rollup/schemas/frames.py` — one `pl.Schema` per logical frame, keyed by
  the enum members.
- `pl.col(C.FOO)` is the project standard — StrEnum members are strings, so
  polars accepts them directly. Attribute shorthand `pl.col.foo` only works
  for valid Python identifiers; some vendor columns have spaces.

### Dynamic column names — driven by `rollup/chain.py`

Year-tagged columns (`f_{tag}`, `loss_..._{tag}_*`) can't
live in a StrEnum because the `{tag}` suffix is data-driven from the
`forecast_factors` seed at runtime. They live in a TypedDict registry:

```python
# rollup/chain.py
class ChainStage(TypedDict):
    suffix:     str        # appended to the running column name
    factor_col: str        # AF.* column or "{tag}"-templated string
    is_per_tag: bool       # is factor_col templated by tag?
    ancillary_before: tuple[str, ...]   # e.g. RNK before EUWS_FACTOR in audit
    ancillary_after:  tuple[str, ...]

CHAIN: dict[str, ChainStage] = {
    "forecast": {"suffix": "",         "factor_col": "f_{tag}",
                 "is_per_tag": True,  ...},
    "euws":     {"suffix": "_euws",    "factor_col": AF.EUWS_FACTOR,
                  "is_per_tag": False, "ancillary_before": (AF.RNK,), ...},
}
```

`add_main_metrics`, `_metric_cols_for`, `audit_wide`, and
`VariantSpec.loss_metric` all walk this registry — adding a new factor stage
is one entry in `CHAIN`, no other edits.

`MetricCol` (in `rollup/schemas/columns.py`) holds the three year-invariant
member names (`LOSS_UPLIFTED`, `LOSS_UPLIFTED_CAPPED`,
`LOSS_UPLIFTED_CAPPED_LOCALCCY`); `chain.CHAIN_BASE` points at the latter
as the chain's starting column.

## Schema validation at every boundary

Three layers, fail fast at each:

1. **Seed load** — `rollup/seeds.py` scans each CSV through
   `pl.scan_csv(..., schema=...)`; `validate_schema` runs on the result.
   Drift → `SchemaError` with a column-level diff.
2. **Model entry + exit** — dbt-layer model functions call
   `validate_schema` on its inputs and outputs.
3. **Plan reporter** — `config.build_plan(cfg)` sniffs each seed's actual
   CSV header against its expected columns *before* the pipeline starts,
   so a drifted seed is caught at the y/N prompt, not mid-run.

## One cached marts node, many fan-outs

The duckdb pipeline materialised `mts_tbl_ylt_combined_all_factors` because
~20 downstream views read from it. We do the same with polars lazy `.cache()`:

```python
intermediate = build_intermediate(cfg, seeds, staging, tags)
all_factors = intermediate.all_factors.cache()          # computed exactly once
fanout_lfs  = [fanout_hisco(all_factors, v) for v in variants]
audit_lfs   = [audit_wide(all_factors, tags),
               audit_long(all_factors, tags)] if dump_interim else []
collected   = pl.collect_all(fanout_lfs + audit_lfs)    # single optimised pass
```

The whole DAG (factors + metrics + fan-outs + audits) goes through one
optimisation pass. Shared work — and all factor computations are shared
across variants — is computed once.

## Audit parquets (`--no-audit` to skip)

Two artefacts are written under `<output_dir>/debug/` by default. Use
`--no-audit` to skip them for faster, smaller runs:

- **`audit_wide.parquet`** — one row per YLT event, columns ordered so the
  factor chain reads left-to-right. Every factor sits next to the metric
  it produces. Layout is registry-driven via `chain.audit_layout_cols`;
  width grows by 2 columns per `CHAIN` entry per forecast tag. Verify
  arithmetic row-by-row by eye.
- **`audit_long.parquet`** — identity + `(metric_name, value)` pairs.
  Pivot-table ready for excel diffs against january's EP summaries.

## Logging

Standard `logging`. Single tree rooted at `rollup`:

- `rollup.seeds` — seed loads.
- `rollup.staging` — YLT parquet scans.
- `rollup.factors` — per-factor stages.
- `rollup.pipeline` — orchestration + event-id checks + fan-out writes.

Default level is `WARNING` (silent for a clean run). Enable info trace via
`--log-level INFO` on the CLI, `[logging].level = "INFO"` in
`rollup.local.toml`, or `ROLLUP_LOG=INFO` env var. Format:
`HH:MM:SS  LEVEL  module  message`.

Sample INFO trace from an end-to-end run:

```
rollup.seeds       loading 11 seeds from …/seeds
rollup.pipeline    plan: 8 Hisco variants across 2 vendors
rollup.pipeline    forecast tags from seed: ['202601', '202607', '202701']
rollup.staging     loaded risklink YLT: …/risklink_ylt_*.parquet
rollup.staging     loaded verisk YLT: …/air_ylt_*.parquet
rollup.pipeline    staging: normalised YLTs concatenated
rollup.pipeline    event-id check (verisk): 80/80 rows matched air_events
rollup.staging     valid analyses filtered YLT inputs
rollup.factors     currency: required_currency derived from CDS class; rate_to_gbp attached
rollup.factors     forecast: 3 factor columns attached — ['f_202601', 'f_202607', 'f_202701']
rollup.factors     euws: factor attached, rank overrides applied from seed
rollup.factors     uplift: rp_bucket proportions + base_model from seed; AAL via window functions
rollup.pipeline    metrics: 9 derived loss columns + 1 dialsup column
rollup.pipeline    fanout: wrote HiscoAIR_202601_main.parquet (80 rows)
...
```

## Event-id orphan count

`count_event_id_orphans(ylt, air_events, vendor_filter)` runs after staging.
Counts `(year_id, event_id, model_code)` triples in the YLT that are NOT
present in `air_events`. Logs `INFO` on clean, `WARNING` on orphans.

This is observation-only — the rollup math doesn't depend on `air_events`,
so orphans don't abort the run. The count is surfaced (returned + logged)
so a downstream check (e.g. cron alert, Slack post) can act on it. If you
want to abort on orphans, wrap the call in your own guard at the call site.

## dbt-layer modules

Each model is a pure function: `(LazyFrame, seed(s)) → LazyFrame`. No side
effects. Composed by the linear DAG in `pipeline.py`. Full list:

- `rollup/staging/ylt.py` — `load_raw_{verisk,risklink}_ylt`,
  `filter_valid_analyses`, `normalize_{verisk,risklink}_ylt`,
  `validate_one_peril_per_rollup_lob`. The normalised
  YLT carries `office` + `lob_class` (from lobs) and
  `peril_name` + `region` + `peril_family` (from perils) so factor stages
  downstream have semantic dims without re-joining. `valid_analyses.csv`
  is the gate: only listed numeric vendor analysis IDs contribute rows.
- `rollup/intermediate/factors.py` — `attach_currency`,
  `attach_forecast_factors`, `attach_rank`, `attach_euws`,
  `attach_uplift`.
- `rollup/intermediate/metrics.py` — `add_main_metrics`, `add_dialsup`.
- `rollup/marts/hisco.py` and `rollup/marts/variants.py` — Hisco fanout
  shape and variant definitions.
- `rollup/reports/summary.py` — operator-facing report model.
- `rollup/staging/ep.py` — `ep_curve_from_ylt` (auxiliary; not in the main
  fan-out chain, but used by integration tests).

## Domain constants — `rollup/config.py`

Closed-set domain values live as `StrEnum` so `pl.col(C) == VendorName.X`
and `pl.lit(VendorName.X)` work as drop-in replacements for raw strings:

| enum            | values                          | where it appears             |
| --------------- | ------------------------------- | ---------------------------- |
| `VendorName`    | `verisk` / `risklink`           | YLT `vendor` column, `base_model`, `analyses.vendor`, `valid_analyses.vendor`, `blending_weights.vendor` |
| `CurrencyCode`  | `GBP` / `EUR`                   | `attach_currency` derivation; values land in `required_currency` |
| `EpType`        | `AAL` / `AEP` / `OEP`           | `EP_TYPE` column emitted by `ep_curve_from_ylt` |
| `EnvVar`        | `ROLLUP_*` env var names        | every `os.getenv` / `monkeypatch.setenv` call |

Plus one non-enum peril-family constant:

- `FLOOD_FAMILY: str = "FL"` — used for flood-specific business rules; runtime
  uplift reads `base_model` from the provided `blending_weights.csv` seed.

See [`factor-chain.md`](factor-chain.md) for how the factor stages compose.
