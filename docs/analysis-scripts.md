# Analysis Scripts

This page contains reusable DuckDB SQL templates for analysts inspecting rollup
outputs. The queries assume the default DuckDB export at `output/rollup.duckdb`.

Run a query from the repository root with DuckDB:

```bash
duckdb output/rollup.duckdb
```

Inside DuckDB, paste any query below. To export query results to CSV:

```sql
COPY (
  SELECT *
  FROM mts_tbl_ylt_combined_all_factors
  LIMIT 100
) TO 'output/analysis/example_extract.csv' (HEADER, DELIMITER ',');
```

The main final metric is `euws_override` in
`mts_tbl_ylt_combined_all_factors`. The DIALSUP final metric is
`dialsup_gbp_forecast` in `mts_tbl_ylt_dialsup`.

## Inventory

### Tables

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'main'
ORDER BY table_name;
```

### Column dictionary

```sql
SELECT
  table_name,
  column_name,
  data_type
FROM information_schema.columns
WHERE table_schema = 'main'
ORDER BY table_name, ordinal_position;
```

### Table row counts

```sql
SELECT 'mts_tbl_ylt_combined_all_factors' AS table_name, COUNT(*) AS rows
FROM mts_tbl_ylt_combined_all_factors
UNION ALL
SELECT 'mts_tbl_ylt_combined_all_factors_wide', COUNT(*)
FROM mts_tbl_ylt_combined_all_factors_wide
UNION ALL
SELECT 'mts_tbl_ylt_dialsup', COUNT(*)
FROM mts_tbl_ylt_dialsup
UNION ALL
SELECT 'cds_fanouts', COUNT(*)
FROM cds_fanouts
UNION ALL
SELECT 'input_ep_summaries', COUNT(*)
FROM input_ep_summaries
ORDER BY table_name;
```

## Portfolio Summaries

### Final main loss by forecast, LOB, and peril

```sql
SELECT
  forecast_date,
  base_model,
  rollup_lob,
  rollup_peril,
  SUM(loss) AS loss
FROM mts_tbl_ylt_combined_all_factors
WHERE metric = 'euws_override'
GROUP BY 1, 2, 3, 4
ORDER BY forecast_date, loss DESC;
```

### DIALSUP loss by forecast, LOB, and peril

```sql
SELECT
  forecast_date,
  base_model,
  rollup_lob,
  rollup_peril,
  SUM(loss) AS loss
FROM mts_tbl_ylt_dialsup
WHERE metric = 'dialsup_gbp_forecast'
GROUP BY 1, 2, 3, 4
ORDER BY forecast_date, loss DESC;
```

### Main vs DIALSUP comparison

```sql
WITH main AS (
  SELECT
    forecast_date,
    base_model,
    rollup_lob,
    rollup_peril,
    SUM(loss) AS main_loss
  FROM mts_tbl_ylt_combined_all_factors
  WHERE metric = 'euws_override'
  GROUP BY 1, 2, 3, 4
),
dialsup AS (
  SELECT
    forecast_date,
    base_model,
    rollup_lob,
    rollup_peril,
    SUM(loss) AS dialsup_loss
  FROM mts_tbl_ylt_dialsup
  WHERE metric = 'dialsup_gbp_forecast'
  GROUP BY 1, 2, 3, 4
)
SELECT
  COALESCE(main.forecast_date, dialsup.forecast_date) AS forecast_date,
  COALESCE(main.base_model, dialsup.base_model) AS base_model,
  COALESCE(main.rollup_lob, dialsup.rollup_lob) AS rollup_lob,
  COALESCE(main.rollup_peril, dialsup.rollup_peril) AS rollup_peril,
  main_loss,
  dialsup_loss,
  dialsup_loss - main_loss AS difference
FROM main
FULL OUTER JOIN dialsup USING (forecast_date, base_model, rollup_lob, rollup_peril)
ORDER BY ABS(difference) DESC NULLS LAST;
```

## Top Losses

### Top main events

```sql
SELECT
  forecast_date,
  base_model,
  rollup_lob,
  rollup_peril,
  year_id,
  event_id,
  model_event_id,
  event_day,
  rnk,
  rp,
  loss
FROM mts_tbl_ylt_combined_all_factors
WHERE metric = 'euws_override'
ORDER BY loss DESC
LIMIT 100;
```

### Top DIALSUP events

```sql
SELECT
  forecast_date,
  base_model,
  rollup_lob,
  rollup_peril,
  year_id,
  event_id,
  model_event_id,
  event_day,
  rnk,
  rp,
  loss
FROM mts_tbl_ylt_dialsup
WHERE metric = 'dialsup_gbp_forecast'
ORDER BY loss DESC
LIMIT 100;
```

## Waterfall And Movement

### Metric waterfall by LOB and peril

```sql
SELECT
  forecast_date,
  base_model,
  rollup_lob,
  rollup_peril,
  metric,
  SUM(loss) AS loss
FROM mts_tbl_ylt_combined_all_factors
GROUP BY 1, 2, 3, 4, 5
ORDER BY forecast_date, base_model, rollup_lob, rollup_peril, metric;
```

### Average uplift factor

```sql
SELECT
  base_model,
  rollup_lob,
  rollup_peril,
  COUNT(*) AS rows,
  AVG(uplift_factor_on_base_model) AS avg_uplift,
  MIN(uplift_factor_on_base_model) AS min_uplift,
  MAX(uplift_factor_on_base_model) AS max_uplift
FROM mts_tbl_ylt_combined_all_factors
WHERE metric = 'euws_override'
GROUP BY 1, 2, 3
ORDER BY avg_uplift DESC;
```

## Wide Output Checks

The wide table contains one column per metric and forecast month. Update the
forecast columns in these examples if the configured forecast dates change.

### Wide profile

```sql
SELECT
  base_model,
  rollup_lob,
  rollup_peril,
  COUNT(*) AS rows,
  SUM(euws_override_202601_loss) AS main_202601,
  SUM(euws_override_202607_loss) AS main_202607,
  SUM(euws_override_202612_loss) AS main_202612,
  SUM(dialsup_gbp_forecast_202601_loss) AS dialsup_202601,
  SUM(dialsup_gbp_forecast_202607_loss) AS dialsup_202607,
  SUM(dialsup_gbp_forecast_202612_loss) AS dialsup_202612
FROM mts_tbl_ylt_combined_all_factors_wide
GROUP BY 1, 2, 3
ORDER BY main_202601 DESC NULLS LAST;
```

### Wide sparsity check

```sql
SELECT
  COUNT(*) AS rows,
  COUNT(euws_override_202601_loss) AS main_202601_populated,
  COUNT(euws_override_202607_loss) AS main_202607_populated,
  COUNT(euws_override_202612_loss) AS main_202612_populated,
  COUNT(dialsup_gbp_forecast_202601_loss) AS dialsup_202601_populated,
  COUNT(dialsup_gbp_forecast_202607_loss) AS dialsup_202607_populated,
  COUNT(dialsup_gbp_forecast_202612_loss) AS dialsup_202612_populated
FROM mts_tbl_ylt_combined_all_factors_wide;
```

## Fanout Audit

### Fanout row counts and loss totals

```sql
SELECT
  fanout_name,
  forecast_yyyymm,
  fanout_metric,
  COUNT(*) AS rows,
  SUM(ModelGrossLoss) AS gross_loss
FROM cds_fanouts
GROUP BY 1, 2, 3
ORDER BY forecast_yyyymm, fanout_name, fanout_metric;
```

### Largest fanout events

```sql
SELECT
  fanout_source_file,
  fanout_name,
  forecast_yyyymm,
  fanout_metric,
  ModelEventID,
  ModelYear,
  ModelEventDay,
  LossClassName,
  ModelGrossLoss
FROM cds_fanouts
ORDER BY ModelGrossLoss DESC
LIMIT 100;
```

## Input And Seed QA

### EP summary coverage

```sql
SELECT
  vendor,
  modelled_lob,
  modelled_peril,
  ep_type,
  COUNT(*) AS rows,
  MIN(return_period) AS min_return_period,
  MAX(return_period) AS max_return_period,
  SUM(loss) AS total_loss
FROM input_ep_summaries
GROUP BY 1, 2, 3, 4
ORDER BY vendor, modelled_lob, modelled_peril, ep_type;
```

### Seed peril selection flags

```sql
SELECT
  rollup_peril,
  base_model,
  COUNT(*) AS candidates,
  SUM(is_dialsup) AS dialsup_candidates,
  SUM(is_euws) AS euws_candidates,
  MIN(selection_priority) AS best_selection_priority
FROM seed_perils
GROUP BY 1, 2
ORDER BY rollup_peril, base_model;
```

### FX rates used

```sql
SELECT *
FROM seed_fx_rates
ORDER BY currency_code, rate_date;
```
