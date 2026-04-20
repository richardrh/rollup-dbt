# polars rollup pipeline

Single-process polars replica of the `jan-rollup` duckdb pipeline. Reads raw
YLT parquets + seed CSVs, applies a chain of factors, fans out to Hisco
parquets. Everything is a `LazyFrame` expression; nothing materialises until
`pl.collect_all` at the sinks.

## Run it

```bash
cd polars
uv run python -m rollup.pipeline --dry-run             # show the plan, exit
uv run python -m rollup.pipeline                       # plan → y/N prompt → run
uv run python -m rollup.pipeline --yes                 # skip prompt, run
uv run python -m rollup.pipeline --yes --dump-interim  # also write audit parquets
uv run python -m rollup.pipeline --yes --log-level INFO# show factor-chain trace
uv run python -m pytest polars/                        # 97 tests, ~1.6s
```

Need to know what data to provide before the run? See
[`../docs/data-requirements.md`](../docs/data-requirements.md) — the canonical
contract between the pipeline and the seeds + YLTs you supply.

## Data flow

```
    raw YLTs                    seeds (11 CSVs)
   ┌─────────┐                 ┌─────────────────────┐
   │ verisk  │                 │ lobs                │
   │risklink │                 │ perils              │  ← split-out
   └────┬────┘                 │ analyses            │  ← god-table
        │                      │ rollup_scope        │  ← gone
        │                      │ blending_weights    │
        │                      │ forecast / fx       │
        │                      │ euws (+overrides)   │
        │                      │ air_events / fa     │
        │                      └──────────┬──────────┘
        ▼                                 │
   ┌──────────────────────────────────────▼───┐
   │ 1. staging → NormalizedYlt (union)       │
   │    + count_event_id_orphans (verisk)     │
   └────────────────────┬─────────────────────┘
                        │
                        ▼
   ┌──────────────────────────────────────────┐
   │ 2. factor chain (one attach_* per factor)│
   │    rollup_scope filter →                 │
   │    FX → forecast(× N tags) → rank →      │
   │    euws (+ rank-threshold overrides)     │
   │    → fa_gross → uplift                   │
   └────────────────────┬─────────────────────┘
                        │
                        ▼
   ┌──────────────────────────────────────────┐
   │ 3. metrics (column name traces chain)    │
   │    loss_uplifted_capped_localccy_{tag}_  │
   │    euws_fagross  +  dialsup_{tag}        │
   └────────────────────┬─────────────────────┘
                        │  .cache()  — single pass
             ┌──────────┴──────────┐
             ▼                     ▼
      Hisco parquets        audit parquets (opt-in)
      Hisco{AIR,RMS}_       audit_wide   (read-across)
      {date}_{main,         audit_long   (pivot-ready)
       dialsup}.parquet
```

## Layout

```
<repo>/
├── docs/                     # detailed docs — see ../docs/README.md
│   ├── README.md
│   ├── data-requirements.md  # the contract for a real run
│   ├── architecture.md
│   ├── factor-chain.md
│   └── calculations.md
└── polars/
    ├── README.md             # this file — overview + run + schematic
    ├── RH-TODO-DATA.md       # punch list for getting real data exported
    ├── rollup/
    │   ├── chain.py          # year-tagged factor chain registry (TypedDict)
    │   ├── config.py         # Vendor + Flavor + VendorName + EnvVar + FLOOD_FAMILY
    │   ├── seeds.py          # typed seed loaders + REQUIRED_SEEDS gate
    │   ├── validate.py       # validate_schema + SchemaError
    │   ├── pipeline.py       # orchestrator + build_all_factors + audit + CLI
    │   ├── schemas/
    │   │   ├── columns.py    # StrEnum per logical frame
    │   │   └── frames.py     # pl.Schema per logical frame
    │   └── stages/
    │       ├── staging.py    # raw YLTs → NormalizedYlt + apply_rollup_scope
    │       ├── factors.py    # attach_* functions (one per factor)
    │       └── ep.py         # YLT → EP curve (aux, not in main chain)
    ├── seeds/                # git-versioned reference CSVs — see seeds/README.md
    └── tests/                # 97 tests including e2e
        ├── test_e2e.py       # the synthetic end-to-end run
        ├── build_test_data.py # generator for tests/data/
        └── data/             # gitignored; test inputs + outputs
```

## Data layout (not in git)

```
<repo>/data/                  # overridable: ROLLUP_DATA_DIR
├── ylt/
│   ├── verisk/*.parquet      # 10,000 simulation years (AIR)
│   └── risklink/*.parquet    # 100,000 simulation years (RMS)
├── ep_summaries/
│   ├── verisk/*.csv
│   └── risklink/*.csv
└── output/                   # pipeline writes Hisco{AIR,RMS}_*.parquet
```

Every path is overridable — `ROLLUP_SEEDS_DIR`, `ROLLUP_YLT_VERISK_DIR`,
`ROLLUP_YLT_RISKLINK_DIR`, `ROLLUP_OUTPUT_DIR`, `ROLLUP_LOG`, etc.

## Docs

- [`../docs/data-requirements.md`](../docs/data-requirements.md) — **start here**.
  Every YLT, seed, and CSV the pipeline needs, with the duckdb `COPY` SQL to
  produce each one. Also: failure-mode reference table.
- [`../docs/architecture.md`](../docs/architecture.md) — code organisation, Vendor /
  Flavor / VariantSpec abstractions, seed loading, schema validation layers.
- [`../docs/factor-chain.md`](../docs/factor-chain.md) — how the factor chain works,
  the cumulative column-naming convention, and the 5-step recipe to add a new
  factor.
- [`../docs/calculations.md`](../docs/calculations.md) — every january duckdb view
  mapped to its polars replacement, with the source SQL quoted.
- [`seeds/README.md`](seeds/README.md) — per-seed schema decisions, column
  naming rules, provenance.
- [`RH-TODO-DATA.md`](RH-TODO-DATA.md) — punch list of duckdb exports the
  user needs to do before a real run.

## Status

Pipeline runs end-to-end on synthetic data. The full chain (staging, factor
attach, metrics, fan-out, audit dumps, interactive CLI) is implemented and
tested. To run on real data, populate the four blocker seeds (perils,
analyses, rollup_scope, blending_weights) listed in
[`../docs/data-requirements.md`](../docs/data-requirements.md) and place the YLT
parquets under `data/ylt/{verisk,risklink}/`.

**97 passing tests in ~1.6s** (`uv run python -m pytest polars/`).
