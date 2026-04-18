# polars pipeline

Single-process polars replica of the `jan-rollup` duckdb pipeline. Everything
is a chain of `LazyFrame` expressions; nothing materializes until the
orchestrator calls `pl.collect_all` at the sinks.

## Why `polars/` (folder) vs `rollup/` (package)

A Python package named `polars` at this repo root would shadow `import polars`
(the library). So the on-disk folder is `polars/` but the importable package
inside is `rollup/`. All internal imports read `from rollup.X import Y`.

```bash
cd polars
python -m rollup.pipeline --dry-run        # show the plan, exit
python -m rollup.pipeline                  # show plan, prompt y/N, then run
python -m rollup.pipeline --yes            # skip prompt, run
python -m pytest tests/
```

`conftest.py` adds this folder to `sys.path` so `import rollup` resolves.

## Layout

```
polars/
├── README.md               # this file
├── calculations.md         # every january → polars stage mapping with SQL
├── RH-TODO-DATA.md         # pending duckdb exports needed to populate seeds
├── conftest.py
├── rollup/
│   ├── config.py           # Vendor dataclass + paths + plan reporter
│   ├── seeds.py            # typed seed loaders (one per CSV)
│   ├── validate.py         # validate_schema + SchemaError
│   ├── pipeline.py         # orchestrator + interactive CLI
│   ├── schemas/
│   │   ├── columns.py      # StrEnum per logical frame
│   │   └── frames.py       # pl.Schema per logical frame
│   └── stages/
│       ├── staging.py      # raw YLTs → NormalizedYlt (per vendor)
│       └── ep.py           # YLT → EP curve (AEP / OEP / AAL)
├── seeds/                  # git-versioned reference CSVs — see seeds/README.md
└── tests/
    ├── test_schemas.py · test_seeds.py · test_config.py
    ├── test_staging.py · test_ep.py · test_pipeline.py
    ├── test_integration_ep.py  # real YLT run, gated on parquet presence
    └── outputs/            # gitignored; integration tests write CSVs here
```

## Data layout (not in git)

```
<repo>/data/
├── ylt/
│   ├── verisk/*.parquet        # 10,000 simulation years (AIR)
│   └── risklink/*.parquet      # 100,000 simulation years (RMS)
├── ep_summaries/
│   ├── verisk/*.csv            # per-LOB / per-peril EP tables
│   └── risklink/*.csv
└── output/                     # pipeline writes HiscoAIR_* / HiscoRMS_*
```

Every path is overridable via the corresponding `ROLLUP_*` env var — see
`rollup/config.py`.

## Vendors — one source of truth

```python
from rollup import config

cfg = config.resolve()
verisk   = cfg.vendor("verisk")    # Vendor(name, hisco_label='AIR', n_simulations=10_000,  ...)
risklink = cfg.vendor("risklink")  # Vendor(name, hisco_label='RMS', n_simulations=100_000, ...)
```

`n_simulations` drives the return-period math in `ep_curve_from_ylt`.
`hisco_label` is the external contract that shows up in output file names
(`HiscoAIR_*.parquet`, `HiscoRMS_*.parquet`).

## Typed columns

- `rollup/schemas/columns.py` — one `StrEnum` per logical frame.
- `rollup/schemas/frames.py` — one `pl.Schema` per logical frame, keyed by
  the enum members.
- `pl.col(C.FOO)` is the project standard — StrEnum members are strings, so
  polars accepts them directly. Attribute shorthand `pl.col.foo` also works
  but only for valid Python identifiers, and some vendor columns have spaces.

## Schema validation everywhere data crosses a boundary

- **At seed load**: `rollup/seeds.py` scans each CSV with its declared
  `pl.Schema`; `validate_schema` runs on the result. Drift fails fast with a
  column-level diff.
- **At stage entry + exit**: every `stages/*.py` function calls
  `validate_schema` on its inputs and outputs.
- **In the plan reporter**: `config.build_plan(cfg)` sniffs each seed's
  actual CSV header against its expected columns *before* the pipeline
  starts, so a drifted seed is caught at the y/N prompt, not halfway through.

## One cached marts node, many fan-outs

The duckdb pipeline materialized `mts_tbl_ylt_combined_all_factors` because
~20 downstream views read from it. We do the same with polars lazy `.cache()`:

```python
all_factors = build_all_factors(...).cache()          # computed exactly once
outputs = [fanout_hisco(all_factors, v) for v in FANOUT_VARIANTS]
collected = pl.collect_all(outputs)                   # single optimized pass
for df, v in zip(collected, FANOUT_VARIANTS):
    df.write_parquet(out / f"{v.name}.parquet")
```

## Seeds

Reference data lives in `seeds/` as git-versioned CSVs. See
[`seeds/README.md`](seeds/README.md) for full schema documentation.

### Optimal dimensional structure

Four new tables replace january's god-dimension (`dim_region_perils`):

| file | rows | purpose |
|---|---|---|
| `perils.csv` | 27 | one row per rollup peril — `peril_id, name, region, peril_family` |
| `analyses.csv` | 7+ | `(vendor, analysis_id)` → `peril_id` [+`lob_id` for RiskLink] |
| `rollup_scope.csv` | stub | `(lob_id, vendor, analysis_id, in_rollup)` — which analyses are in scope per LOB |
| `blending_weights.csv` | 50 | long-format `(peril_id, sub_peril, vendor, weight)` |

The legacy seeds (`dim_region_perils.csv`, `dim_risklink_analysis.csv`) remain
during transition. They will be retired once the optimal structure is wired
through the staging code.

Pending exports that will populate the stubs are tracked in
[`RH-TODO-DATA.md`](RH-TODO-DATA.md).

## Documentation

- [`calculations.md`](calculations.md) — every january duckdb view mapped to a
  polars stage, with the original SQL quoted. Includes the official-rollup
  selection logic (section 9) and the reference-data source-of-truth notes
  (section 10).
- [`seeds/README.md`](seeds/README.md) — seed schema decisions, column-naming
  rules, and per-file provenance.
- [`RH-TODO-DATA.md`](RH-TODO-DATA.md) — copy-pasteable duckdb SQL for each
  pending seed export.

## Status

Done:
- Schemas: raw YLTs (RiskLink + Verisk), dim/ref tables, NormalizedYlt,
  EpCurve, AllFactors, Metrics, Hisco.
- `validate_schema` (strict and non-strict).
- `load_raw_risklink_ylt`, `load_raw_verisk_ylt`, `normalize_risklink_ylt`.
- `ep_curve_from_ylt` (validated on real 4.4M-row Verisk YLT).
- `FANOUT_VARIANTS` (21 variants) + `fanout_hisco` projection.
- `pipeline.main()` CLI: plan → prompt → run.
- Typed seed loader bundle (`seeds.load_all`).
- Per-seed schema validation in the plan reporter.
- Optimal 4-table seed structure (`perils`, `analyses`, `rollup_scope`,
  `blending_weights`) — schemas wired, CSVs seeded (analyses + rollup_scope
  still partially stubbed pending duckdb exports).

Stub (`NotImplementedError`):
- `build_all_factors` — composes the 5 middle stages.

TODO (middle stages, in dependency order):
1. `stages/staging.py::normalize_verisk_ylt`
2. `stages/funnel.py` — rank + bucket + validity filter
3. `stages/blending.py` — vendor proportions → uplift factor clipped [0.1, 10]
4. `stages/forecast.py` — forecast factors per LOB + FX to local ccy
5. `stages/euws.py` — per-event EUWS factor
6. `stages/fa_gross.py` — fine-art AAL / tail adjustments
7. Wire `build_all_factors` to call the above in order

See [`calculations.md`](calculations.md) for the full january → polars mapping
with SQL and per-stage status.

Tests: **68 passing** (`uv run --project .. python -m pytest tests/` from `polars/`).
