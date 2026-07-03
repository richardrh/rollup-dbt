-- Portfolio-level final loss summaries.

-- Final main loss by forecast, LOB, and peril.
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

-- Final DIALSUP loss by forecast, LOB, and peril.
SELECT
  forecast_date,
  base_model,
  rollup_lob,
  rollup_peril,
  SUM(loss) AS loss
FROM mts_tbl_ylt_dialsup
WHERE metric = 'dialsup_localccy_forecast'
GROUP BY 1, 2, 3, 4
ORDER BY forecast_date, loss DESC;

-- Main vs DIALSUP comparison.
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
  WHERE metric = 'dialsup_localccy_forecast'
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
