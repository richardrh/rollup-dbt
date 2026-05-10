# EP Summaries

## Purpose

These files feed `rollup derive-blending`, which computes per-peril AAL-weighted
blending proportions between RiskLink and Verisk models. Without EP summaries,
the blending seed (`data/seeds/vor/blending_weights.csv`) must be hand-edited.

With EP summaries, you can regenerate blending weights automatically whenever
the vendor models are refreshed:

```bash
uv run rollup ep-summary-to-csv              # convert xlsx → long CSV
uv run rollup derive-blending                # compute weights from AALs
```

---

## End-to-end workflow

### 1. Source data — where it comes from

**RiskLink (RMS):** Vendor exports an analysis list export (`.xlsx`) containing
per-peril AAL and exceedance probability (EP) curves. File is typically named
`rms_analysis_list.xlsx` or similar.

**Verisk (AIR):** No automated export. You must manually produce the long CSV
from AIR Touchstone export or equivalent loss analysis output.

### 2. File structure — where files live

```
data/ep_summaries/
├── risklink/          ← RiskLink (RMS) CSVs only after conversion
│   ├── rms_ep_summary.xlsx           ← source (xlsx kept for reference, optional)
│   └── rms_ep_summary.long.csv       ← converted output (REQUIRED)
└── verisk/            ← Verisk (AIR) CSVs — must be produced manually
    └── air_ep_summary.long.csv       ← manually produced (REQUIRED)
```

The pipeline reads `*.long.csv` files from each vendor folder. Original xlsx
files may coexist but are ignored after conversion.

### 3. Convert RiskLink xlsx → long CSV

Copy your RMS xlsx export to `data/ep_summaries/risklink/`:

```bash
cp rms_analysis_list.xlsx data/ep_summaries/risklink/
```

Run the converter:

```bash
uv run rollup ep-summary-to-csv
```

This reads the xlsx multi-row header and wide return-period columns, and writes
a sibling `.long.csv` file with the same stem name:

```
data/ep_summaries/risklink/rms_analysis_list.xlsx  ← input
data/ep_summaries/risklink/rms_analysis_list.long.csv  ← output
```

### 4. Produce Verisk long CSV manually

Verisk doesn't have an automated converter. You must produce the long CSV directly
from AIR Touchstone export or equivalent.

**What Touchstone typically exports:**
- One row per analysis × LOB combination
- Columns: `Analysis` (e.g. `EU_WS`), `ExposureAttribute` (LOB), and wide return-period
  columns like `OEP_2`, `OEP_5`, ..., `AEP_1000`

**Transform to long format:**

Long format has one row per (analysis, lob, return_period, ep_type). You can do this in
several ways:

**Option A: polars one-liner**

```python
import polars as pl

wide = pl.read_csv("air_touchstone_export.csv")
# Unpivot OEP_N and AEP_N columns
rp_cols = [c for c in wide.columns if c.startswith(("OEP_", "AEP_"))]
long = (
    wide
    .with_columns(
        pl.when(pl.col("Analysis").is_not_null())
        .then(pl.lit(0)).otherwise(pl.lit(None)).alias("rp"),
        pl.lit("AAL").alias("ep_type"),
        pl.col("AAL").alias("gl")
    )
    .select("rp", "ep_type", "Analysis", "ExposureAttribute", "gl")
    .rename({"Analysis": "analysis", "ExposureAttribute": "lob"})
    .union(
        wide.unpivot(
            index=["Analysis", "ExposureAttribute"],
            variable_name="rp_ep",
            value_name="gl"
        )
        .with_columns(
            pl.col("rp_ep").str.extract(r"^([A-Z]+)_(\d+)$", 1).alias("ep_type"),
            pl.col("rp_ep").str.extract(r"^([A-Z]+)_(\d+)$", 2).cast(pl.Int64).alias("rp")
        )
        .select("rp", "ep_type", "Analysis", "ExposureAttribute", "gl")
        .rename({"Analysis": "analysis", "ExposureAttribute": "lob"})
        .drop("rp_ep")
    )
    .filter(pl.col("gl").is_not_null())
)
long.write_csv("data/ep_summaries/verisk/air_ep_summary.long.csv")
```

**Option B: Excel pivot table + manual export**

1. Open the Touchstone export in Excel.
2. Create a pivot table with Analysis × LOB on rows, return periods on columns, values = GL.
3. Use Excels' unpivot feature (or manually copy-paste) to create the long format.
4. Export as CSV to `data/ep_summaries/verisk/air_ep_summary.long.csv`.

**Option C: Python template (general)**

```python
import pandas as pd

# Read Touchstone export
df = pd.read_csv("air_touchstone_export.csv")

# Melt/unpivot wide columns to long
rp_cols = [c for c in df.columns if c.startswith(("OEP_", "AEP_"))]
long = pd.melt(
    df,
    id_vars=["Analysis", "ExposureAttribute"],
    value_vars=rp_cols,
    var_name="rp_ep_type",
    value_name="gl"
)

# Extract EP type (OEP/AEP) and return period
long[["ep_type", "rp"]] = long["rp_ep_type"].str.extract(r"^([A-Z]+)_(\d+)$")
long["rp"] = long["rp"].astype(int)

# Add AAL rows (rp=0, ep_type=AAL)
aal_rows = df[["Analysis", "ExposureAttribute", "AAL"]].copy()
aal_rows.columns = ["Analysis", "ExposureAttribute", "gl"]
aal_rows["rp"] = 0
aal_rows["ep_type"] = "AAL"

# Combine and save
result = pd.concat([aal_rows, long[["rp", "ep_type", "Analysis", "ExposureAttribute", "gl"]]])
result.columns = ["analysis", "lob", "gl", "rp", "ep_type"]
result = result[["rp", "ep_type", "analysis", "lob", "gl"]]
result.to_csv("data/ep_summaries/verisk/air_ep_summary.long.csv", index=False)
```

### 5. Validate and regenerate blending weights

After both RiskLink and Verisk CSVs are in place, validate with a dry run:

```bash
uv run rollup --dry-run
```

If the `[ep_summaries]` section shows ✓ for both vendors, regenerate the
blending weights:

```bash
uv run rollup derive-blending
```

This computes per-peril AAL totals and writes `data/seeds/vor/blending_weights.csv`
with proportions:

```
rl_proportion = rl_aal / (rl_aal + vk_aal)
vk_proportion = 1 - rl_proportion
```

Run dry-run again to confirm the new weights are loaded.

---

## Schema

### RiskLink — `risklink/*.long.csv`

| column | type | notes |
|--------|------|-------|
| `id` | integer | RiskLink analysis ID from the xlsx |
| `rp` | integer | return period; `0` = AAL row |
| `ep_type` | string | `AAL`, `OEP`, or `AEP` |
| `lob` | string | modelled LOB — must match `analyses.modelled_label` |
| `region_peril` | string | peril label — must match `analyses.modelled_label` |
| `gl` | float | gross loss |

**Sample:**
```
id,rp,ep_type,lob,region_peril,gl
1,0,AAL,HIC_HH_UK,GB FL HD,1806464.0
1,2,OEP,HIC_HH_UK,GB FL HD,321433.0
1,100,OEP,HIC_HH_UK,GB FL HD,19365339.0
1,1000,OEP,HIC_HH_UK,GB FL HD,62873626.0
1,0,AAL,HIC_HH_UK,GB WSSS,10775338.0
1,100,OEP,HIC_HH_UK,GB WSSS,81040832.0
```

**Auto-convert:** `uv run rollup ep-summary-to-csv <file>.xlsx`

### Verisk — `verisk/*.long.csv`

| column | type | notes |
|--------|------|-------|
| `rp` | integer | return period; `0` = AAL row |
| `ep_type` | string | `AAL`, `OEP`, or `AEP` |
| `analysis` | string | Verisk analysis label — must match `analyses.modelled_label` |
| `lob` | string | modelled LOB |
| `gl` | float | gross loss |

**Sample:**
```
rp,ep_type,analysis,lob,gl
0,AAL,EU_WS,HIC_HH_UK,5421000.0
2,OEP,EU_WS,HIC_HH_UK,1043000.0
100,OEP,EU_WS,HIC_HH_UK,18200000.0
250,OEP,EU_WS,HIC_HH_UK,24100000.0
1000,OEP,EU_WS,HIC_HH_UK,34400000.0
0,AAL,GB_FL,HIC_HH_UK,1650000.0
100,OEP,GB_FL,HIC_HH_UK,17800000.0
```

**Manual production:** Touchstone export → unpivot → CSV (see Section 4 above).

---

## Return period set

`rp` must be one of: `0, 2, 5, 10, 20, 25, 50, 100, 200, 250, 500, 1000, 10000`

Missing return periods are silently skipped (sparse files are fine). Only AAL
rows (`ep_type=AAL`, `rp=0`) are used for blending weight derivation; the
full curve is available for future diagnostics and validation.

---

## Common mistakes

1. **Column names are case-sensitive and exact.**
   - RiskLink: `id`, `rp`, `ep_type`, `lob`, `region_peril`, `gl` (lowercase)
   - Verisk: `rp`, `ep_type`, `analysis`, `lob`, `gl` (lowercase)
   - Typos → silent join failures (zero output rows)

2. **Values must match `analyses.csv` exactly.**
   - `lob` must match `analyses.modelled_label` for that LOB
   - `region_peril` (RiskLink) or `analysis` (Verisk) must match `analyses.modelled_label`
   - Case and spacing matter (e.g. `GB FL HD` not `GB FL` or `GBFLHD`)
   - Silent join misses → zero output from `derive-blending`

3. **AAL rows are critical.**
   - `rp=0, ep_type=AAL` rows are required for blending weight derivation
   - If any peril is missing AAL rows, its blending weight will be computed as 0
   - Check that every peril in both vendors has an AAL row

4. **Negative or null gross losses.**
   - Keep only non-negative GL values
   - NULLs are ignored by polars' aggregation, which is fine
   - Zero GL is valid (e.g. very low probability perils in some analyses)

5. **File naming.**
   - RiskLink: must end in `.long.csv` (e.g. `rms_ep_summary.long.csv`)
   - Verisk: must end in `.long.csv` (e.g. `air_ep_summary.long.csv`)
   - The glob pattern is `*.long.csv` — any other extension is ignored

---

## Next steps

- **Section 3 of [load-data.md](../docs/load-data.md)** — procedural checklist for collecting EP summaries
- **[data-requirements.md](../docs/data-requirements.md#a0-ep-summary-xlsx--converting-to-long-format-csv)** — schema reference for `blending_weights.csv`
