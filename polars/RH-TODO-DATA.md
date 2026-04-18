# RH — pending data exports

These are the data exports blocking the pipeline. Each produces a CSV that
drops into `polars/seeds/`. All are
`COPY (SELECT ...) TO 'path.csv' WITH (HEADER, DELIMITER ',')`
statements against the duckdb file that powers the january rollup. The schema
dump for that database is at `jan-rollup/duckdb_schema/table_definitions.csv`.

Run each `COPY` statement from a duckdb shell opened on the january database,
then drop the resulting CSV into `polars/seeds/`. `python -m rollup.pipeline
--dry-run` (from `polars/`) will show a row count instead of `(stub)` once
each seed is populated.

---

## Critical (blocks the new optimal seed structure)

- [ ] **`rollup_scope.csv`** — which (lob_id, vendor, analysis_id) pairs are in
  the official rollup for each LOB type.

  **Background**: `official_rollup` in january is not a stored flag — it is
  computed on-the-fly in `vw_ep` as:

  ```sql
  CASE lob_type
    WHEN 'mga'  THEN applies_to_mga
    WHEN 'prop' THEN applies_to_prop
    WHEN 'fa'   THEN applies_to_fa
    ELSE 0
  END AS official_rollup
  ```

  So the source of truth is the `applies_to_{mga,prop,fa}` columns on each
  row of `dim_region_perils`. The correct grain for `rollup_scope` is
  `(lob_id, vendor, analysis_id)` — **not** `(lob_id, peril_id)` — because
  two analyses can share a `peril_id` (e.g. `UK_WSSS` and `UK_WSSS_GCAdj`
  are both peril 206 but only the GCAdj variant has `applies_to_*=1`).

  Note: the current `rollup_scope.csv` header is `lob_id,peril_id,in_rollup`.
  **Rename it** to `lob_id,vendor,analysis_id,in_rollup` before running this
  export (update `rollup/schemas/columns.py` and `frames.py` to match).

  ```sql
  COPY (
    SELECT
      lobs.id                   AS lob_id,
      rp.vendor                 AS vendor,
      rp.modelled_region_peril  AS analysis_id,
      CASE lobs.lob_type
        WHEN 'mga'  THEN rp.applies_to_mga
        WHEN 'prop' THEN rp.applies_to_prop
        WHEN 'fa'   THEN rp.applies_to_fa
        ELSE 0
      END                       AS in_rollup
    FROM reference.lobs AS lobs
    CROSS JOIN loader.main.dim_region_perils AS rp
    ORDER BY lobs.id, rp.vendor, rp.modelled_region_peril
  ) TO 'polars/seeds/rollup_scope.csv' WITH (HEADER, DELIMITER ',');
  ```

  Post-processing: no column renames needed — output is already snake_case.

- [ ] **`analyses.csv` — RiskLink rows** — append the RiskLink analysis→peril
  mapping to the existing 7 Verisk rows in `polars/seeds/analyses.csv`.

  The existing file has columns: `vendor, analysis_id, modelled_label, peril_id, lob_id`.
  For RiskLink, `analysis_id` and `modelled_label` are both the
  `rl_analysis_id` (integer), and `lob_id` is populated (unlike Verisk where
  it is NULL because a Verisk analysis spans all LOBs for that peril).

  ```sql
  COPY (
    SELECT
      'risklink'                       AS vendor,
      CAST(dra.rl_analysis_id AS VARCHAR) AS analysis_id,
      dra.region_peril                 AS modelled_label,
      rp.id                            AS peril_id,
      lobs.id                          AS lob_id
    FROM loader.main.dim_rl_analysis AS dra
    INNER JOIN loader.main.dim_region_perils AS rp
      ON rp.modelled_region_peril = dra.region_peril
      AND rp.vendor = 'rl'
    INNER JOIN reference.lobs AS lobs
      ON lobs.modelled_lob = dra.lob
    ORDER BY dra.rl_analysis_id
  ) TO '/tmp/analyses_rl.csv' WITH (HEADER, DELIMITER ',');
  ```

  Post-processing: manually append the rows from `/tmp/analyses_rl.csv`
  (minus its header) to `polars/seeds/analyses.csv`. The 7 Verisk rows stay
  at the top; RiskLink rows follow. There should be one row per
  (rl_analysis_id, lob) combination.

---

## High (blocks staging + euws + fanout)

- [ ] **`air_events.csv`** — AIR event catalogue; needed for the EUWS factor
  lookup and for `ModelEventDay` in the Verisk fan-out.

  Output columns: `event_id, model_id, event, year, day`

  ```sql
  COPY (
    SELECT
      EventID  AS event_id,
      ModelID  AS model_id,
      "Event"  AS event,
      "Year"   AS year,
      "Day"    AS day
    FROM reference.air_events
    ORDER BY event_id
  ) TO 'polars/seeds/air_events.csv' WITH (HEADER, DELIMITER ',');
  ```

  Post-processing: none — column rename is done in the SELECT.

- [ ] **`fineart_adjustments.csv`** — fine-art gross-to-net AAL and tail
  factors; needed for the `fa_gross` stage.

  Output columns: `lob_id, region_peril_id, applies_to_fa, rollup_region_peril, aal_factor, tail_factor`

  ```sql
  COPY (
    SELECT
      lob_id,
      region_peril_id,
      applies_to_fa,
      rollup_region_peril,
      aal_factor,
      tail_factor
    FROM reference.fineart_gross_to_net_adjustment2
    ORDER BY lob_id, region_peril_id
  ) TO 'polars/seeds/fineart_adjustments.csv' WITH (HEADER, DELIMITER ',');
  ```

  Post-processing: none — source columns are already snake_case.

- [ ] **`flood_rl22_model_events.csv`** — RiskLink RL22 flood event table;
  needed for `ModelEventDay` in the RiskLink flood fan-out
  (`fanout_rl_withdayid`).

  Output columns: `model_event_pk, model_provider_id, model_event_id, model_occurrence_year, model_occurrence_date, region_peril_id`

  ```sql
  COPY (
    SELECT
      ModelEventPK        AS model_event_pk,
      ModelProviderID     AS model_provider_id,
      ModelEventID        AS model_event_id,
      ModelOccurrenceYear AS model_occurrence_year,
      ModelOccurrenceDate AS model_occurrence_date,
      RegionPerilID       AS region_peril_id
    FROM reference.flood_rl22_model_events
    ORDER BY model_event_pk
  ) TO 'polars/seeds/flood_rl22_model_events.csv' WITH (HEADER, DELIMITER ',');
  ```

  Post-processing: `ModelOccurrenceDate` is a TIMESTAMP in duckdb — duckdb
  will export it as an ISO-8601 string. The polars schema expects
  `pl.Datetime`; `pl.scan_csv(..., schema=...)` will parse it automatically
  if the format is `YYYY-MM-DD HH:MM:SS`.

---

## Low (may not be needed)

- [ ] **`cds_region_peril.csv`** — CDS region-peril mapping; used in some
  verify views but not in the main calculation chain.

  Output columns: `id, cds_region_peril, cds_sub_region_peril, cds_model_to_use`

  ```sql
  COPY (
    SELECT
      id,
      cds_region_peril,
      cds_sub_region_peril,
      cds_model_to_use
    FROM reference.cds_region_peril
    ORDER BY id
  ) TO 'polars/seeds/cds_region_peril.csv' WITH (HEADER, DELIMITER ',');
  ```

  Post-processing: none — source columns are already snake_case.
