-- Wide MTS template for one rollup LOB/peril.
-- The wide table contains dimensions, blend diagnostics, and final forecast loss
-- columns. Use sql/04_mts_long_lob_peril_waterfall.sql for the full
-- original -> blended -> localccy -> forecast -> EUWS -> final waterfall.
-- First run sql/01_inventory.sql and replace the example YYYYMM columns below
-- with columns present in mts_tbl_ylt_combined_all_factors_wide.
-- Replace CHANGE_ME values before running.

SELECT
  base_model,
  rollup_lob,
  rollup_peril,
  modelled_lob,
  modelled_peril,
  year_id,
  event_id,
  model_event_id,
  event_day,
  rnk,
  rp,

  -- Blend diagnostics carried from the main final row.
  risklink_blended_contribution,
  verisk_blended_contribution,
  uplift_factor_on_base_model,

  -- Final forecast losses. Replace 202601 with a forecast month in your DB.
  euws_override_202601_loss,
  dialsup_localccy_forecast_202601_loss
FROM mts_tbl_ylt_combined_all_factors_wide
WHERE rollup_lob = 'CHANGE_ME'
  AND rollup_peril = 'CHANGE_ME'
ORDER BY
  euws_override_202601_loss DESC NULLS LAST,
  dialsup_localccy_forecast_202601_loss DESC NULLS LAST,
  base_model,
  year_id,
  event_id
LIMIT 500;

-- Portfolio-level wide check for the same LOB/peril and forecast month.
SELECT
  base_model,
  rollup_lob,
  rollup_peril,
  COUNT(*) AS rows,
  AVG(uplift_factor_on_base_model) AS avg_blend_factor,
  SUM(euws_override_202601_loss) AS final_main_loss,
  SUM(dialsup_localccy_forecast_202601_loss) AS final_dialsup_loss
FROM mts_tbl_ylt_combined_all_factors_wide
WHERE rollup_lob = 'CHANGE_ME'
  AND rollup_peril = 'CHANGE_ME'
GROUP BY 1, 2, 3
ORDER BY final_main_loss DESC NULLS LAST;
