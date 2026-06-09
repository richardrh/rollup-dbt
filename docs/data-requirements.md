# Data requirements

Inputs live under `data/`. Generated files live under `output/`. The runtime
validates source availability plus required schema/nullability for the main
inputs before running calculations.

## Required analyst inputs

| Data | Location |
| --- | --- |
| Verisk YLT parquet | `data/ylt/verisk/*.parquet` |
| RiskLink YLT parquet | `data/ylt/risklink/*.parquet` |
| EP summary long CSVs | `data/ep_summaries/**/*.long.csv` |
| Seeds | `data/seeds/**` |

Expected layout:

```text
data/
  ylt/verisk/*.parquet
  ylt/risklink/*.parquet
  ep_summaries/**/*.long.csv
  seeds/business/lobs.csv
  seeds/business/perils.csv
  seeds/vor/blending_factors.csv
  seeds/vor/fx_rates.csv
  seeds/vor/forecast_factors.csv
  seeds/vor/euws_rate_factors.csv
  seeds/adjustments/euws_rank_overrides.csv
  seeds/validation/verisk_events.parquet
  seeds/validation/risklink_flood22_model_events.parquet
```

YLT contracts support multiple parquet files per vendor. The loader reads all
direct `*.parquet` files in `data/ylt/verisk/` and `data/ylt/risklink/`; each
vendor folder must contain at least one matching parquet file. File names do not
need to follow a naming convention, but meaningful names are recommended for
operator traceability. Keep inactive/test parquet files out of these active
folders because they will be loaded. Subdirectories are not scanned.

EP summaries used by the pipeline must be long CSVs with exactly:

```text
vendor,analysis_id,modelled_lob,modelled_peril,ep_type,return_period,loss
```

Raw YLT parquet inputs may include extra vendor-export columns. Validation only
requires the useful columns consumed by the pipeline:

- Verisk: `Analysis`, `ExposureAttribute`, `CatalogTypeCode`, `EventID`,
  `ModelCode`, `YearID`, `GroundUpLoss`. A row-level `filename` column is not
  required; validation reports derive file names from parquet paths.
- RiskLink: `anlsid`, `yearid`, `eventid`, `loss`. Diagnostic columns such as
  `p_value`, `meanloss`, `stddev`, and `expvalue` are optional.

Every RiskLink YLT `anlsid` must match a RiskLink EP summary `analysis_id` after
string casting.

## Creating EP summary long CSVs from wide CSVs

Use `rollup generate-ep-summaries` when an analyst or vendor gives you a wide
CSV instead of a `.long.csv` file.

If the vendor provides an Excel file (`.xlsx`), the tool does not support Excel
directly. Open the file in Excel or another spreadsheet application, save the
relevant sheet as a CSV file (e.g. "Save As" → "CSV UTF-8 (Comma delimited)
(*.csv)"), then place that CSV in the vendor folder and proceed with the steps
below.

### Step 1. Put the source CSV in the vendor folder

```text
data/ep_summaries/verisk/*.csv
data/ep_summaries/risklink/*.csv
```

Existing `*.long.csv` files are ignored during source selection. RiskLink uses
the same source CSV format as Verisk. If a vendor folder contains multiple source
wide CSVs, pass `--vendor` and `--csv`; scan mode fails rather than guessing.

### Step 2. Run the converter

Interactive:

```bash
uv run rollup generate-ep-summaries
```

Non-interactive:

```bash
uv run rollup generate-ep-summaries --vendor verisk --csv verisk_clean.csv
uv run rollup generate-ep-summaries --vendor risklink --csv risklink_clean.csv
```

### Step 3. Check the generated `.long.csv`

Output paths are fixed by vendor:

- Verisk: `data/ep_summaries/verisk/verisk_ep_summary.long.csv`
- RiskLink: `data/ep_summaries/risklink/rms_ep_summary.long.csv`

### Step 4. Validate

```bash
uv run python -m rollup run --data-root data --output-root output --target-currency GBP --no-stage-outputs --no-analysis
```

### Source wide CSV format

Source CSVs must have their header on row 1. Use one row per analysis, LOB, and
peril, with EP losses in metric columns.

| Column | Required? | Description | Example/Notes |
| --- | --- | --- | --- |
| `id` | Yes | Analysis identifier. The converter writes this as `analysis_id` in the long CSV. | `ANALYSIS_1`, `101` |
| `modelled_lob` | Yes | Modelled line of business. It must exist in `lobs.csv` before the pipeline runs. | `Property` |
| `modelled_peril` | Yes | Modelled peril. It must exist in `perils.csv` before the pipeline runs. | `US_WS` |
| Metric columns | Yes, at least one | Loss values to turn into long rows. Names must follow `<EP type>_<return period>` for `AAL`, `AEP`, or `OEP`. | Preferred names are uppercase with no `.0` suffix, such as `AAL_0`, `AEP_50`, `OEP_100`. `AAL` always becomes return period `0`. |
| `ExposureAttribute` | Optional alias | Accepted instead of `modelled_lob` when `modelled_lob` is not present. | Common in Verisk exports. |
| `Analysis` | Optional alias | Accepted instead of `modelled_peril` when `modelled_peril` is not present. | Common in Verisk exports. |
| `CatalogTypeCode` | Optional filter | If present, only rows where the trimmed value is `STC` are converted. Other rows are skipped. | `STC` |
| `segment` | Optional | Accepted but not used in the output. | Ignored by the converter. |
| `sd*` columns | Optional | Accepted but not used in the output. | `sd_0`, `sd_0.0`; ignored by the converter. |

New source files should use metric names like `AAL_0`, `AEP_50`, and `OEP_100`.
The converter also accepts lowercase metric names and `.0` suffixes from older
exports.

Example source CSV:

```csv
id,modelled_lob,modelled_peril,AAL_0,AEP_50,OEP_100
ANALYSIS_1,Property,US_WS,1250,1750472,2250000
```

Rows with blank IDs, LOBs, perils, or losses are skipped.

### Generated long CSV format

The converter writes exactly these columns:

| Column | Source/Derivation | Description |
| --- | --- | --- |
| `vendor` | CLI vendor selection (`verisk` or `risklink`) | Vendor name used by the pipeline. |
| `analysis_id` | Source `id` | Analysis identifier from the wide CSV. |
| `modelled_lob` | Source `modelled_lob`, or `ExposureAttribute` alias | Modelled line of business. |
| `modelled_peril` | Source `modelled_peril`, or `Analysis` alias | Modelled peril. |
| `ep_type` | Metric column name before the underscore | EP metric type: `AAL`, `AEP`, or `OEP`. |
| `return_period` | Metric column number after the underscore | Return period as a number. `AAL` is always `0`. |
| `loss` | Metric column value | Loss value with commas and spaces removed, written as a floating-point number. |

Example generated rows from the source above:

```csv
vendor,analysis_id,modelled_lob,modelled_peril,ep_type,return_period,loss
verisk,ANALYSIS_1,Property,US_WS,AAL,0,1250.0
verisk,ANALYSIS_1,Property,US_WS,AEP,50,1750472.0
verisk,ANALYSIS_1,Property,US_WS,OEP,100,2250000.0
```

### Source validation config

Do not add a separate YAML schema file for source wide CSVs yet. The converter
checks the required identifier columns and at least one EP metric column. The
existing `data/ep_summaries/validnator.yml` describes the long CSV files used by
remote validation workflows. The runtime enforces the long CSV shape with a
strict Pandera schema.

## Seed files

Seed schema contracts are documented in colocated Validnator configs; see
[Validnator contracts](schema-contracts.md). Runtime validation uses strict
Pandera schemas and reports seed shape issues before calculations start.

### `data/seeds/business/lobs.csv`

Business LOB lookup. It maps vendor/modelled LOB values in `modelled_lob` to
`rollup_lob`, `lob_type`, CDS class, office, class, and currency.

The pipeline uses it to enrich EP summaries and YLT rows. Downstream class,
office, and currency-driven logic depends on this file, including forecast and FX
application.

### `data/seeds/business/perils.csv`

Peril lookup from GC/vendor `modelled_peril` values to rollup peril labels,
region/peril labels, and `region_peril_id`.

`selection_priority` chooses the main pipeline's preferred modelled peril variant
when multiple modelled perils map to the same vendor, `rollup_lob`, and
`rollup_peril`. Lower numbers win. Missing priorities are filled as `99` during
EP staging; schema text uses `99` as the normal fallback priority, and some
calling contexts treat the fallback/default as `99`/`100`.

`is_dialsup` is the independent DIALSUP peril-selection flag and is preserved at
rollup peril level. The main pipeline ignores this flag and continues to use
`selection_priority`. DIALSUP output can differ from the main output when this
flag selects a different source peril.

### `data/seeds/vor/blending_factors.csv`

VOR blend weights by `RegionPerilID` and `SubRegionPerilID`. The EP blend target
step uses the AIR and RMS weights to create blended loss targets from Verisk and
RiskLink EP summaries.

Europe Flood `RegionPerilID` `216` is special-cased to use
`SubRegionPerilID` `216b`.

### `data/seeds/vor/fx_rates.csv`

FX lookup from source currency to target currency. The target currency is
explicit and defaults to `GBP`. A non-empty FX seed must include the requested
target currency. Missing non-target rates fail rather than silently defaulting.

### `data/seeds/vor/forecast_factors.csv`

Forecast multipliers by class, office, and forecast date. The pipeline expands
YLT rows across available forecast dates and applies the matching factor.

Missing class/office/date factors default to `1.0`.

### `data/seeds/vor/euws_rate_factors.csv`

Event-level Europe Windstorm factors by model event and occurrence year. These
are applied only to `Europe_WS` rows after joining YLT events to the Verisk event
catalogue. Non-Europe Windstorm rows use factor `1.0`.

### `data/seeds/adjustments/euws_rank_overrides.csv`

Override file for selected zero EUWS factors. It applies by `rollup_lob` to
top-ranked rows where the raw EUWS factor is zero and the row rank is within the
configured maximum rank.

### `data/seeds/validation/verisk_events.parquet`

Validation/event catalogue mapping Verisk event, year, and model code to model
event id and event day. It supports EUWS factor joins, DIALSUP, and event-date
fields in outputs.

### `data/seeds/validation/risklink_flood22_model_events.parquet`

RiskLink flood event occurrence-date catalogue. The mart fanout uses it to derive
event day fields for RiskLink flood rows.

## Validation checklist

- Every EP `modelled_lob` and YLT modelled LOB must exist in `lobs.csv`.
- Every EP `modelled_peril` and YLT modelled peril must exist in `perils.csv`.
- Inputs must match the runtime Pandera schemas for required files, columns, and
  types. The colocated YAML contracts document the same intended shapes for
  remote validation workflows.
- Extra raw YLT vendor columns are allowed, but seed and EP summary files remain
  strict and should not contain unexpected columns.
- Every RiskLink YLT `anlsid` must exist in the RiskLink EP summary
  `analysis_id` values.
- Seed entries without matching EP/YLT data do not produce output by themselves.
- Run `validate_rollup_inputs("data")` or a no-output-analysis smoke run; treat
  validation failures as blocking
  input or lookup errors.
