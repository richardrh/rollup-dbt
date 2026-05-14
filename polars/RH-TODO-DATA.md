# Data collection checklist

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
        ├── risklink/    ← RiskLink long CSVs (*.long.csv)
        └── verisk/      ← Verisk long CSVs (*.long.csv)
```

---

## EP summaries — long-format CSVs

EP summaries feed `rollup derive-blending`, which computes AAL-weighted blending
proportions between RiskLink and Verisk for every peril. The pipeline reads the
files in `data/ep_summaries/<vendor>/` as CSV and expects the **long format**
described below — one row per (lob, region_peril / analysis, return period,
ep_type). This is different from the wide format that RMS exports natively
(one column per return period). Conversion instructions are at the bottom of
this section.

The two vendors use slightly different schemas because Verisk identifies a peril
with a single `analysis` label (e.g. `EU_WS`) whereas RiskLink uses a
numeric `id` plus a `region_peril` string (e.g. `GB FL HD`).

---

### RiskLink EP summary

**File location:** `data/ep_summaries/risklink/<name>.long.csv`

**File naming:** any name ending in `.long.csv` — e.g. `rms_ep_summary.long.csv`.
The pipeline globs `*.long.csv` so you can split by model year if needed.

| column | type | allowed values | notes |
|--------|------|---------------|-------|
| `id` | integer | any positive int | RiskLink analysis ID (from the xlsx export) |
| `rp` | integer | `0, 2, 5, 10, 20, 25, 50, 100, 200, 250, 500, 1000, 10000` | `0` means this is an AAL row |
| `ep_type` | string | `AAL`, `OEP`, `AEP` | AAL rows always have `rp=0` |
| `lob` | string | any | modelled LOB label — must match `analyses.csv` |
| `region_peril` | string | any | the peril label — joined to `analyses.modelled_label` |
| `gl` | float | any non-negative | gross loss in the model currency |

**Sample rows:**

```
id,rp,ep_type,lob,region_peril,gl
1,0,AAL,HIC_HH_UK,GB FL HD,1806464.0
1,100,OEP,HIC_HH_UK,GB FL HD,19365339.0
1,200,OEP,HIC_HH_UK,GB FL HD,29108147.0
1,1000,OEP,HIC_HH_UK,GB FL HD,62873626.0
1,0,AAL,HIC_HH_UK,GB WSSS,10775338.0
```

---

### Verisk EP summary

**File location:** `data/ep_summaries/verisk/<name>.long.csv`

**File naming:** any name ending in `.long.csv` — e.g. `air_ep_summary.long.csv`.

| column | type | allowed values | notes |
|--------|------|---------------|-------|
| `rp` | integer | `0, 2, 5, 10, 20, 25, 50, 100, 200, 250, 500, 1000, 10000` | `0` means this is an AAL row |
| `ep_type` | string | `AAL`, `OEP`, `AEP` | AAL rows always have `rp=0` |
| `analysis` | string | any | the Verisk analysis label — must match `analyses.modelled_label` |
| `lob` | string | any | modelled LOB label |
| `gl` | float | any non-negative | gross loss in the model currency |

**Sample rows:**

```
rp,ep_type,analysis,lob,gl
0,AAL,EU_WS,HIC_HH_UK,5421000.0
100,OEP,EU_WS,HIC_HH_UK,18200000.0
250,OEP,EU_WS,HIC_HH_UK,24100000.0
0,AAL,GB_FL,HIC_HH_UK,1650000.0
100,OEP,GB_FL,HIC_HH_UK,17800000.0
```

---

### Converting from wide format (what RMS exports naturally)

RMS exports a wide CSV with one column per return period, like
`rms_analysis_list.csv`:

```
ID, Segment, LOB, RegionPeril, AAL, STD, OEP_2, OEP_5, ..., AEP_10000
```

The `rollup ep-summary-to-csv` command reads the **xlsx** version of this and
writes the long CSV for you:

```
rollup ep-summary-to-csv data/ep_summaries/risklink/rms_ep_summary.xlsx
# writes: data/ep_summaries/risklink/rms_ep_summary.long.csv
```

If you already have a clean wide CSV (no Excel formatting, no comma-in-number
quoting), you can convert it in Python:

```python
import polars as pl

wide = pl.read_csv("rms_analysis_list.csv")
# melt OEP_N / AEP_N columns
rp_cols = [c for c in wide.columns if c.startswith(("OEP_", "AEP_"))]
long = (
    wide
    .rename({"ID": "id", "LOB": "lob", "RegionPeril": "region_peril", "AAL": "_aal"})
    .with_columns(pl.col("id").cast(pl.Int64))
    # AAL rows first
    .select("id", "lob", "region_peril", "_aal")
    .rename({"_aal": "gl"})
    .with_columns(pl.lit("AAL").alias("ep_type"), pl.lit(0).alias("rp"))
)
# then pivot the OEP/AEP columns and concat — or just use rollup ep-summary-to-csv.
```

For most cases, just use the xlsx and run `rollup ep-summary-to-csv` — that
handles the Excel number formatting (e.g. `"1,806,464"`) for you.

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
| `analysis_id` | string | numeric ID, e.g. `900003` / `501` |
| `modelled_label` | string | `EU_WS` |
| `peril_id` | integer | `206` |
| `lob_id` | integer or empty | `3` (RiskLink only — leave blank for Verisk rows) |

---

### `data/seeds/valid_analyses.csv`

Which numeric vendor analysis IDs are in the official rollup.

| column | type | example |
|--------|------|---------|
| `vendor` | string | `verisk` or `risklink` |
| `analysis_id` | string | numeric ID, e.g. `900003` or `501` |

`analysis_id` here must match the vendor-native numeric ID for both vendors.
Verisk raw `Analysis` labels belong in `analyses.modelled_label`.

---

### `data/seeds/blending_weights.csv`

| column | type | example |
|--------|------|---------|
| `peril_id` | integer | `216` |
| `return_period` | integer | `0`, `200`, `1000`, or `10000` |
| `peril_name` | string | `Europe Flood` |
| `description` | string | `default 50/50` |
| `sub_peril` | string or empty | `216a` |
| `vendor` | string | `verisk` or `risklink` |
| `base_model` | string | `risklink` |
| `weight` | float | `0.5` |

Every peril and return-period bucket needs both a `verisk` row and a
`risklink` row. Runtime buckets are `0` (AAL), `200` (1-in-200), `1000`
(1-in-1000), and `10000` (1-in-10000+).

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

### `data/ylt/risklink/risklink_ylt*.parquet`

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
- `valid_analyses.analysis_id` must match numeric vendor-native IDs, not display labels.
- `analyses.lob_id`: blank for Verisk rows, populated for RiskLink rows.
- `forecast_factors.office` must match `lobs.office` exactly (case + spacing).

---

Ping me when done and I'll run the pipeline.
