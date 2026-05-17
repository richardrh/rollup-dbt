# polars rollup pipeline

Single-process polars replica of the `jan-rollup` duckdb pipeline. Reads raw
YLT parquets + seed CSVs, applies a chain of factors, fans out to Hisco
parquets. Everything is a `LazyFrame` expression; nothing materialises until
`pl.collect_all` at the sinks.

## Run it

```bash
# from repo root
uv run rollup --dry-run                  # show the plan/coverage, exit
uv run rollup                            # interactive operator wizard
uv run rollup --yes                      # non-interactive run
uv run rollup --yes --no-audit           # skip debug audit parquets
uv run rollup --yes --min-loss 0         # disable default loss filter (keep every row)
uv run rollup --yes --log-level INFO     # show factor-chain trace
uv run rollup ep-summary-to-csv          # convert wide xlsx → long CSV
uv run rollup test-sql                   # probe SQL connection (read-only)
uv run rollup push-to-sql                # push 8 Hisco parquets to SQL Server
uv run rollup docs                       # open the docs site in your browser
uv run pytest -q                         # run the default suite (integration skipped by default)
```

`python -m rollup` is equivalent.

## Experimental pipeline2 track

`rollup.pipeline2` is a separate, experimental clean path. Its dataset
contracts live under `../data/` next to the inputs they describe:
`../data/seeds/schema.yaml`, `../data/ylt/schema.yaml`,
`../data/ep_summaries/schema.yaml`, and `../data/output/schema.yaml`. Edit those
data-side manifests when seed, YLT, EP-summary, or pipeline2 output shapes
change; there is no authoritative schema YAML hidden under `rollup/`. The code
only uses the new YAML schema helper plus Polars. It does not import the legacy
runtime modules, and the existing `rollup.pipeline` CLI/runtime remains
unchanged for now.

The initial pipeline2 scope is intentionally small: load YAML-declared sources,
validate columns strictly, stage raw YLT rows into a minimal canonical shape,
filter to first-class `selected_analyses` (with `valid_analyses` only as a
legacy fallback), and produce a demonstrable loss-summary mart. It is a safe
foundation for a future dbt-style linear DAG rather than a port of all legacy
business transformations.

Need to know what data to provide before the run? See
[`../docs/load-data.md`](../docs/load-data.md) for a step-by-step procedural walkthrough, or
[`../docs/data-requirements.md`](../docs/data-requirements.md) for the canonical
contract between the pipeline and the seeds + YLTs you supply.

## Data flow

```
    raw YLTs                    seeds (11 CSVs)
   ┌─────────┐                 ┌─────────────────────┐
   │ verisk  │                 │ lobs                │
   │risklink │                 │ perils              │  ← split-out
   └────┬────┘                 │ analyses            │  ← god-table
        │                      │ valid_analyses      │  ← allow-list
        │                      │ blending_weights    │
        │                      │ forecast / fx       │
        │                      │ euws (+overrides)   │
        │                      │ event catalogues    │
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
   │    valid analysis gate →                 │
   │    FX → forecast(× N tags) → rank →      │
   │    euws (+ rank-threshold overrides)     │
   │    → uplift                              │
   └────────────────────┬─────────────────────┘
                        │
                        ▼
   ┌──────────────────────────────────────────┐
   │ 3. metrics (column name traces chain)    │
   │    loss_uplifted_capped_localccy_{tag}_  │
   │    euws  +  dialsup                      │
   └────────────────────┬─────────────────────┘
                        │  .cache()  — single pass
             ┌──────────┴──────────┐
             ▼                     ▼
      Hisco parquets        audit parquets (opt-in)
      Hisco{AIR,RMS}_       audit_wide   (read-across)
      {date}_{main,         audit_long   (pivot-ready)
       dialsup}.parquet
```

## Layout — source code vs user-owned data

```
<repo>/
├── docs/                       # detailed docs — see ../docs/index.md
│   ├── README.md
│   ├── data-requirements.md    # the contract for a real run
│   ├── architecture.md
│   ├── factor-chain.md
│   └── calculations.md
│
├── polars/                     # SOURCE CODE — don't put data in here
│   ├── README.md               # this file
│   ├── RH-TODO-DATA.md         # checklist for collecting real data
│   ├── rollup/
│   │   ├── chain.py            # year-tagged factor chain registry (TypedDict)
│   │   ├── config.py           # Vendor + Flavor + VendorName + EnvVar
│   │   ├── seeds.py            # typed seed loaders + REQUIRED_SEEDS gate
│   │   ├── validate.py         # validate_schema + SchemaError
│   │   ├── pipeline.py         # linear DAG orchestrator + audit + CLI
│   │   ├── schemas/
│   │   │   ├── columns.py      # StrEnum per logical frame
│   │   │   └── frames.py       # pl.Schema per logical frame
│   │   ├── staging/            # raw vendor inputs → typed staging models
│   │   │   ├── ylt.py          # valid analyses + raw YLTs → NormalizedYlt
│   │   │   └── ep.py           # YLT → EP curve (aux, not in main chain)
│   │   ├── intermediate/       # factor attachment + derived metrics
│   │   ├── marts/              # Hisco fanout + variant specs
│   │   ├── reports/            # summary report model
│   │   └── io/                 # output writers and external sinks
│   └── tests/                  # 241 passing tests including e2e
│       ├── test_e2e.py         # the synthetic end-to-end run
│       ├── build_test_data.py  # generator for tests/data/
│       └── data/               # gitignored; test inputs + outputs
│
└── data/                       # USER-OWNED — this is what you populate
    ├── seeds/                  # reference CSVs — see data/seeds/README.md
    │   └── schema.yaml         # pipeline2 seed contracts live with seeds
    │                             + RH-TODO-DATA.md for the 4 blockers
    ├── ylt/
    │   ├── schema.yaml         # pipeline2 raw YLT contracts live with inputs
    │   ├── verisk/*.parquet    # 10,000 simulation years (AIR)
    │   └── risklink/*.parquet  # 100,000 simulation years (RMS)
    ├── ep_summaries/           # optional review inputs
    │   ├── schema.yaml         # pipeline2 EP summary contracts live here
    │   ├── verisk/*.long.csv
    │   └── risklink/*.long.csv
    └── output/                 # pipeline writes Hisco{AIR,RMS}_*.parquet
        └── schema.yaml         # pipeline2 staging/intermediate/mart contracts
```

Every path is overridable — `ROLLUP_SEEDS_DIR`, `ROLLUP_YLT_VERISK_DIR`,
`ROLLUP_YLT_RISKLINK_DIR`, `ROLLUP_OUTPUT_DIR`, `ROLLUP_LOG`, etc. See
`rollup/config.py::EnvVar` for the full list.

## Setup

```bash
# from repo root
uv sync                                 # install dependencies
cp rollup.example.toml rollup.local.toml # local config (gitignored; edit SQL/min_loss/paths)

# (Optional) Tab completion for bash/zsh — add to ~/.bashrc or ~/.zshrc for persistence
eval "$(register-python-argcomplete rollup)"
```

## Docs

- [`analyst-demo.html`](analyst-demo.html) — self-contained browser carousel for
  walking an analyst through the morning demo runbook.
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
- [`../data/seeds/README.md`](../data/seeds/README.md) — per-seed schema
  decisions, column naming rules, provenance.
- [`../docs/load-data.md`](../docs/load-data.md) — **operator runbook** with exact commands
  for seeds, YLTs, EP summaries, dry-run, wizard, and output checks.
- [`RH-TODO-DATA.md`](RH-TODO-DATA.md) — **simple checklist for the
  data-collection pass** (what files, what columns, where to put them).

### Build a self-contained single-page HTML

Pandoc is already installed system-wide and available in the dev environment:

```bash
pandoc \
  docs/index.md docs/file-formats.md docs/data-requirements.md \
  docs/architecture.md docs/factor-chain.md docs/calculations.md \
  docs/operating-modes.md \
  -o rollup-pipeline-docs.html \
  --standalone --embed-resources \
  --toc --toc-depth=2 \
  --metadata title="Polars Rollup Pipeline"
```

This produces a single self-contained HTML file (works from a USB stick or email).

## Status

Pipeline runs end-to-end on synthetic data. The full chain (staging, factor
attach, metrics, fan-out, audit dumps, interactive CLI) is implemented and
tested. To run on real data, work through
[`../docs/load-data.md`](../docs/load-data.md) — collect the run-scope seeds,
replace the bundled Verisk placeholder analysis IDs, drop YLT parquets under
`data/ylt/{verisk,risklink}/`, and provide EP-summary `*.long.csv` files for
default blending derivation.

Run `uv run pytest polars/ -q` for the default suite. Integration tests require
extra local services/data and are skipped by default; opt in with
`--run-integration` when those dependencies are available.
