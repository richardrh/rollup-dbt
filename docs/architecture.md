# Architecture

The rollup pipeline is a file-based batch process. Analysts drop source inputs
under `data/`; the CLI validates them, enriches vendor YLT rows with seed
lookups, derives EP-driven blend factors, applies business factors, and writes
mart/report outputs under root `output/`.

## Data flow

```mermaid
flowchart TD
  A[Analyst drops inputs under data/] --> B[Schemas and validation]
  B --> C[Seed lookups\nlobs.csv, perils.csv, VOR factors, validation catalogues]
  B --> D[EP summary staging\nenrich and select preferred peril]
  B --> E[YLT normalization\nVerisk and RiskLink]
  C --> D
  C --> E
  D --> F[EP join and blending targets\nAAL, OEP 200, OEP 1000]
  E --> G[YLT enrichment\nLOB, peril, class, office, currency]
  F --> H[Base-model YLT blending\nrank, RP bucket, uplift]
  G --> H
  H --> I[FX, forecast, EUWS factors\nand EUWS overrides]
  H --> J[DIALSUP branch\nbase-model YLT + FX/forecast]
  I --> K[Main mart fanout]
  J --> L[DIALSUP mart fanout]
  K --> M[Wide MTS outputs]
  L --> M
  M --> N[EP analysis report]
```

## Pipeline phases

| Phase | What happens | Debug prefix |
| --- | --- | --- |
| Seed + validation | Read seed files, event catalogues, YLTs, and EP summaries; report schema and lookup coverage issues. | `seed_*`, `stg_validation_*` |
| Staging | Normalize YLT formats and stage EP summaries with LOB/peril enrichment and preferred modelled peril selection. | `stg_*` |
| Intermediate | Join EP vendors, calculate blend targets, enrich YLT rows, apply blending, FX, forecast, EUWS, and build DIALSUP. | `int_*` |
| Marts | Build main/DIALSUP fanouts, event validation, and wide MTS outputs. | `mts_*` |

Normal runs write only final outputs. Use `uv run rollup run --debug` when you
need intermediate parquet frames in `output/debug/`.
