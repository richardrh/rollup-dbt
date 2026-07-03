-- Waterfall and wide-output checks.

-- Metric waterfall by forecast, LOB, and peril.
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

-- Average uplift factor for final main rows.
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

-- Wide output profile. Update forecast month columns if forecast dates change.
SELECT
  base_model,
  rollup_lob,
  rollup_peril,
  COUNT(*) AS rows,
  SUM(euws_override_202601_loss) AS main_202601,
  SUM(euws_override_202607_loss) AS main_202607,
  SUM(euws_override_202612_loss) AS main_202612,
  SUM(dialsup_localccy_forecast_202601_loss) AS dialsup_202601,
  SUM(dialsup_localccy_forecast_202607_loss) AS dialsup_202607,
  SUM(dialsup_localccy_forecast_202612_loss) AS dialsup_202612
FROM mts_tbl_ylt_combined_all_factors_wide
GROUP BY 1, 2, 3
ORDER BY main_202601 DESC NULLS LAST;

-- Wide output sparsity check. Update forecast month columns if forecast dates change.
SELECT
  COUNT(*) AS rows,
  COUNT(euws_override_202601_loss) AS main_202601_populated,
  COUNT(euws_override_202607_loss) AS main_202607_populated,
  COUNT(euws_override_202612_loss) AS main_202612_populated,
  COUNT(dialsup_localccy_forecast_202601_loss) AS dialsup_202601_populated,
  COUNT(dialsup_localccy_forecast_202607_loss) AS dialsup_202607_populated,
  COUNT(dialsup_localccy_forecast_202612_loss) AS dialsup_202612_populated
FROM mts_tbl_ylt_combined_all_factors_wide;
