-- Largest final events.

-- Top main events.
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

-- Top DIALSUP events.
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
WHERE metric = 'dialsup_localccy_forecast'
ORDER BY loss DESC
LIMIT 100;
