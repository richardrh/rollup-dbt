# polars rollup pipeline

Single-process polars-native rollup pipeline. Reads YLT parquets + seed CSVs,
applies a chain of factors, fans out to Hisco parquets. Everything is a
`LazyFrame` expression; nothing materialises until `pl.collect_all` at the sinks.

## Run it

```bash
# from repo root
uv run rollup --dry-run                  # show the plan, exit
uv run rollup                            # plan → y/N prompt → run
uv run rollup --yes                      # skip prompt, run
uv run rollup --yes --dump-interim       # also write audit parquets
uv run rollup --yes --min-loss 0         # disable default loss filter (keep every row)
uv run rollup --yes --log-level INFO     # show factor-chain trace
uv run rollup ep-summary-to-csv          # convert wide xlsx → long CSV
uv run rollup derive-blending            # rewrite blending_weights from EP AALs
uv run rollup test-sql                   # probe SQL connection (read-only)
uv run rollup push-to-sql                # push 8 Hisco parquets to SQL Server
uv run rollup docs                       # open the docs site in your browser
uv run pytest -q                         # 150 unit + 6 integration tests, ~5s (integration skipped by default)
```

`python -m rollup` is equivalent.

Need to know what data to provide before the run? See
[`../docs/load-data.md`](../docs/load-data.md) for a step-by-step procedural walkthrough, or
[`../docs/data-requirements.md`](../docs/data-requirements.md) for the canonical
contract between the pipeline and the seeds + YLTs you supply.

## Data flow

```
    raw YLTs                    seeds (11 CSVs)
   ┌─────────┐                 ┌─────────────────────┐
   │ verisk  │                 │ lobs                │
   │risklink │                 │ perils              │  ← split into
   └────┬────┘                 │ analyses            │  ← four tables
        │                      │ rollup_scope        │
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

## Layout — source code vs user-owned data

```
<repo>/
├── docs/                       # detailed docs — see ../docs/README.md
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
│   │   ├── config.py           # Vendor + Flavor + VendorName + EnvVar + FLOOD_FAMILY
│   │   ├── seeds.py            # typed seed loaders + REQUIRED_SEEDS gate
│   │   ├── validate.py         # validate_schema + SchemaError
│   │   ├── pipeline.py         # orchestrator + build_all_factors + audit + CLI
│   │   ├── schemas/
│   │   │   ├── columns.py      # StrEnum per logical frame
│   │   │   └── frames.py       # pl.Schema per logical frame
│   │   └── stages/
│   │       ├── staging.py      # raw YLTs → NormalizedYlt + apply_rollup_scope
│   │       ├── factors.py      # attach_* functions (one per factor)
│   │       └── ep.py           # YLT → EP curve (aux, not in main chain)
│   └── tests/                  # 97 tests including e2e
│       ├── test_e2e.py         # the synthetic end-to-end run
│       ├── build_test_data.py  # generator for tests/data/
│       └── data/               # gitignored; test inputs + outputs
│
└── data/                       # USER-OWNED — this is what you populate
    ├── seeds/                  # reference CSVs — see data/seeds/README.md
    │                             + RH-TODO-DATA.md for the 4 blockers
    ├── ylt/
    │   ├── verisk/*.parquet    # 10,000 simulation years (AIR)
    │   └── risklink/*.parquet  # 100,000 simulation years (RMS)
    ├── ep_summaries/           # optional; only used by integration tests
    │   ├── verisk/*.csv
    │   └── risklink/*.csv
    └── output/                 # pipeline writes Hisco{AIR,RMS}_*.parquet
```

Every path is overridable — `ROLLUP_SEEDS_DIR`, `ROLLUP_YLT_VERISK_DIR`,
`ROLLUP_YLT_RISKLINK_DIR`, `ROLLUP_OUTPUT_DIR`, `ROLLUP_LOG`, etc. See
`rollup/config.py::EnvVar` for the full list.

## Setup

```bash
# from repo root
uv sync                                 # install dependencies
cp config.example.py config.py          # local config (gitignored; edit to set MSSQL_CONN_STR or MIN_LOSS)

# (Optional) Tab completion for bash/zsh — add to ~/.bashrc or ~/.zshrc for persistence
eval "$(register-python-argcomplete rollup)"
```

## Docs

- [`../docs/data-requirements.md`](../docs/data-requirements.md) — **start here**.
  Every YLT, seed, and CSV the pipeline needs, with the duckdb `COPY` SQL to
  produce each one. Also: failure-mode reference table.
- [`../docs/architecture.md`](../docs/architecture.md) — code organisation, Vendor /
  Flavor / VariantSpec abstractions, seed loading, schema validation layers.
- [`../docs/factor-chain.md`](../docs/factor-chain.md) — how the factor chain works,
  the cumulative column-naming convention, and the 5-step recipe to add a new
  factor.
- [`../docs/calculations.md`](../docs/calculations.md) — polars stage modules
  that compute the loss chain, with reference SQL quoted.
- [`../data/seeds/README.md`](../data/seeds/README.md) — per-seed schema
  decisions, column naming rules, provenance.
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
[`RH-TODO-DATA.md`](RH-TODO-DATA.md) — collect the four blocker seed
CSVs into `data/seeds/` and drop the YLT parquets under
`data/ylt/{verisk,risklink}/`.

**~150 unit tests + 6 integration tests** (`uv run python -m pytest polars/`). Integration tests require Docker and are skipped by default; opt-in with `--run-integration`. Unit tests run in ~5s.
