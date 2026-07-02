# Adding LOBs and perils

The pipeline maps vendor/modelled LOB and peril values to canonical rollup labels
using two business seed files. Every LOB and peril in your EP summaries and YLT
data must exist in these files before the pipeline runs, or `rollup validate`
will report anti-join failures.

## Seed files

### `data/seeds/business/lobs.csv`

Business LOB lookup. Columns:

| Column | Description | Example |
| --- | --- | --- |
| `lob_id` | Unique row identifier (integer) | `1` |
| `modelled_lob` | Vendor/modelled LOB value as it appears in EP summaries and YLT data | `HSA_HH_EU_IE` |
| `rollup_lob` | Rollup LOB label used in downstream joins and output | `HSA_HH_EU_IE` |
| `lob_type` | `prop` (property) or `cas` (casualty) | `prop` |
| `cds_cat_class_name` | CDS catastrophe class name used in FX and forecast joins | `HSA EU Household` |
| `office` | Office code | `IE` |
| `class` | Class code | `HH` |
| `currency` | ISO currency code | `EUR` |

### `data/seeds/business/perils.csv`

Peril lookup. Columns:

| Column | Description | Example |
| --- | --- | --- |
| `modelled_peril` | Vendor/modelled peril value as it appears in EP summaries and YLT data | `BE FL` |
| `rollup_peril` | Rollup peril label used in downstream joins and output | `Belgium_FL` |
| `region` | Region label | `Belgium` |
| `peril` | Base peril code | `FL` |
| `region_peril_id` | Integer identifier for region-peril combination used in blend weight joins | `216` |
| `selection_priority` | Main-pipeline precedence for choosing among multiple modelled perils that map to the same vendor, `rollup_lob`, and `rollup_peril`. Lower numbers win. Missing values default to `99`. | `99` |
| `is_dialsup` | DIALSUP-only selection flag. Exactly one active candidate per vendor, `rollup_lob`, and `rollup_peril` must be `1`; adjusted alternatives should usually be `0`. | `1` |

## Adding a new LOB

Suppose a new portfolio segment `Emerging Markets` arrives with modelled LOB
value `HSA_FA_EM`. You need a matching row in `lobs.csv`.

1.  Open `data/seeds/business/lobs.csv`.
2.  Add a new row with the next available `lob_id`:

    ```csv
    64,HSA_FA_EM,HSA_FA_EM,prop,HSA FA Emerging Markets,EM,FA,EUR
    ```

    The pipeline uses `rollup_lob` for all downstream joins, so if you need a
    different canonical name, set `rollup_lob` separately from `modelled_lob`.

3.  If the LOB uses a different currency, office, class, or `lob_type`, adjust
    those columns. The forecast, FX, and class-based logic depend on these
    values.
4.  If the portfolio has multiple modelled LOB variants that should map to the
    same rollup LOB (e.g. `HSA_FA_EM` and `HSA_FA_EM_V2`), add one row per
    variant. All will map to the same downstream label.

## Adding a new peril

Suppose your EP summary contains a new peril value `ES_FL` (Spain Flood) that
does not yet exist in `perils.csv`.

1.  Open `data/seeds/business/perils.csv`.
2.  Add a new row:

    ```csv
    ES_FL,Spain_FL,Spain,FL,218,99,1
    ```

    - `region_peril_id` must be unique. Check the existing rows for the highest
      value and increment. This ID is used in VOR blending, so choose a stable
      value.
    - `selection_priority` of `99` is the normal fallback for the main pipeline.
      Set a lower number (e.g. `1`) if this modelled peril should be preferred
      over other modelled perils that target the same `rollup_peril`.
    - `is_dialsup` should be `1` for the least-adjusted/base peril that DIALSUP
      should use, and `0` for adjusted alternatives. Validation requires exactly
      one active DIALSUP candidate per vendor/rollup LOB/rollup peril group.

3.  If your EP data uses a different column name for perils (e.g. `Analysis`
      instead of `modelled_peril`), the EP summary converter accepts that alias
      automatically. No change needed in the peril lookup — just ensure the
      actual peril value appears in `perils.csv`.

## Handling multiple modelled perils for the same rollup peril

When two modelled perils map to the same `rollup_lob` + `rollup_peril`
combination (e.g. `UK_WSSS` and `UK_WSSS_GCAdj` → `UK_WS`), the main pipeline
picks one using `selection_priority`:

- Lower number wins.
- Missing priority defaults to `99`.
- The losing modelled peril rows are excluded from the blended EP target and
  YTL blending steps.

If you want to change which modelled peril is preferred, update the
`selection_priority` values for the relevant rows in `perils.csv`.

DIALSUP is separate: it uses `is_dialsup = 1`, not the main priority winner, so
it can keep using the base peril even when the main pipeline selects an adjusted
variant. If DIALSUP chooses a different modelled peril, DIALSUP output can have
different row counts or sparser wide-output values than the main output.

## Validation

After editing either file, run:

```bash
uv run rollup validate
```

The validation report includes a **Modelled LOB/peril anti-join check** that
lists every EP summary and YLT modelled LOB/peril value that has no matching
row in `lobs.csv` or `perils.csv`. The anti-join report must be empty before
running the pipeline.

The anti-join only checks data-to-seed direction: values in EP summaries or YLTs
that are missing from `lobs.csv` or `perils.csv`. Adding a LOB or peril to a seed
file that has no matching data produces no error — the entry is silently ignored
downstream. To make a new LOB or peril actually flow through the pipeline, it must
also appear in an EP summary or YLT input file.

```text
Modelled LOB/peril anti-join report   ← check this section in the output
shape: (0, 14)                        ← zero rows means all LOBs and perils match
```

If the anti-join report has rows, either:

- Add the missing values to `lobs.csv` / `perils.csv` (recommended if the data
  is legitimate), or
- Correct the EP summary or YLT source data if the values are spurious.

## See also

- [Data requirements](data-requirements.md#seed-files) — full seed file reference
- Validnator YAMLs under `data/` — external required columns and types
- [Troubleshooting](troubleshooting.md) — common LOB/peril mismatch issues
