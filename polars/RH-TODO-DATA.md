# Data collection checklist

# TODO: I don't know if i have done it right or not? i have
# dropped a risklink parquet file into./data/ylt/risklink - it contains GB Flood
# and GB Wind but not yet BE or DE Flood which we originally mapped to Europe FLood
# the EP Summaries - i need to know what format we need. They originally came
# from excel spreadsheets but i think we should define what format we want
# in csv format. then the user knows what they need to produce.
# e.g. see ./rms_analysis_list.csv ? 


Put these files in the right place and ping me. That's it.

All paths below are inside the **repo root** (`rollup-dbt/`) — the same folder that contains `polars/`, `jan-rollup/`, etc.

```
rollup-dbt/              ← repo root (this is where you cloned it)
├── polars/              ← source code — don't touch
├── jan-rollup/          ← legacy reference — don't touch
└── data/                ← YOUR DATA GOES HERE
    ├── seeds/           ← reference CSVs
    ├── ylt/
    │   ├── verisk/      ← Verisk AIR YLT parquets
    │   └── risklink/    ← RiskLink YLT parquets
    └── ep_summaries/
        └── risklink/    ← RMS comparison spreadsheets (already here)
```

---

## Blocker seeds — pipeline refuses to run without these

### `data/seeds/perils.csv`

| column | type | example |
|--------|------|---------|
| `peril_id` | integer | `206` |
| `name` | string | `Europe Winter Storm` |
| `region` | string | `EU` |
| `peril_family` | string | `WS` |

`peril_family` values: `WS`, `FL`, `EQ`, `TC`, `CS`, `WF`. **Flood must be exactly `FL` (uppercase).**

---

### `data/seeds/analyses.csv`

| column | type | example |
|--------|------|---------|
| `vendor` | string | `verisk` or `risklink` |
| `analysis_id` | string | `EU_WS` / `501` |
| `modelled_label` | string | `EU_WS` |
| `peril_id` | integer | `206` |
| `lob_id` | integer or empty | `3` (RiskLink only — leave blank for Verisk rows) |

---

### `data/seeds/rollup_scope.csv`

Which (lob, vendor, analysis) combinations are in the official rollup.

| column | type | example |
|--------|------|---------|
| `lob_id` | integer | `3` |
| `vendor` | string | `verisk` or `risklink` |
| `analysis_id` | string | `EU_WS` |
| `in_rollup` | boolean | `true` |

`analysis_id` here must match `modelled_label` from `analyses.csv` — not the raw RiskLink integer.

---

### `data/seeds/blending_weights.csv`

| column | type | example |
|--------|------|---------|
| `peril_id` | integer | `216` |
| `peril_name` | string | `Europe Flood` |
| `description` | string | `default 50/50` |
| `sub_peril` | string or empty | `216a` |
| `vendor` | string | `verisk` or `risklink` |
| `weight` | float | `0.5` |

Every peril needs both a `verisk` row and a `risklink` row.

---

## YLT parquets

### `data/ylt/verisk/air_ylt_*.parquet`

Multiple chunk files fine — the pipeline globs them all.

| column | type |
|--------|------|
| `Analysis` | string |
| `ExposureAttribute` | string |
| `CatalogTypeCode` | string |
| `EventID` | integer |
| `ModelCode` | integer |
| `YearID` | integer |
| `PerilSetCode` | integer |
| `GroundUpLoss` | float |
| `GrossLoss` | float |
| `NetOfPreCatLoss` | float |
| `filename` | string |

Pipeline uses `NetOfPreCatLoss` as the loss column. Filters to `CatalogTypeCode = 'STC'`.

### `data/ylt/risklink/risklink_ylt_*.parquet`

| column | type |
|--------|------|
| `SimulationSetId` | integer |
| `yearid` | integer |
| `eventid` | integer |
| `date` | string |
| `p_value` | float |
| `anlsid` | integer |
| `name` | string |
| `description` | string |
| `rate` | float |
| `meanloss` | float |
| `stddev` | float |
| `expvalue` | float |
| `loss` | float |

Pipeline uses `loss`. `anlsid` is cast to string and joined to `analyses.analysis_id`.

---

## FX rates — replace the stub

`data/seeds/fx_rates.csv` has 6 placeholder rows. Replace with a real snapshot.

| column | type | example |
|--------|------|---------|
| `currency_code` | string | `EUR` |
| `target_currency` | string | `GBP` |
| `rate_date` | date | `2026-01-01` |
| `rate` | float | `0.88` |

You need at minimum `GBP→GBP = 1.0` and `EUR→GBP = <rate>`.

---

## Gotchas

- `peril_family` must be `"FL"` not `"Flood"` / `"fl"` / `"FL "`.
- `vendor` must be lowercase: `verisk` / `risklink`.
- `rollup_scope.analysis_id` must match `analyses.modelled_label` (e.g. `EU_WS`), not the raw RiskLink integer.
- `analyses.lob_id`: blank for Verisk rows, populated for RiskLink rows.
- `forecast_factors.office` must match `lobs.office` exactly (case + spacing).

---

Ping me when done and I'll run the pipeline.
