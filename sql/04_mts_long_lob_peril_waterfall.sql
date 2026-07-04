-- Long MTS waterfall template for one rollup LOB/peril.
-- This is the best template for reading the transform path left-to-right.
-- Replace CHANGE_ME values before running. Update the forecast-date columns if
-- the configured forecast dates change.

-- Portfolio waterfall: original loss through each main transform, with forecast
-- dates pivoted into columns so static transforms and average blend factor stay
-- on the same row.
SELECT
  base_model,
  rollup_lob,
  rollup_peril,
  SUM(loss) FILTER (WHERE metric = 'original') AS original_loss,
  AVG(uplift_factor_on_base_model) FILTER (WHERE metric = 'blended') AS avg_blend_factor,
  SUM(loss) FILTER (WHERE metric = 'blended') AS blended_loss,
  SUM(loss) FILTER (WHERE metric = 'localccy') AS localccy_loss,

  SUM(loss) FILTER (WHERE metric = 'localccy_forecast' AND forecast_date = DATE '2026-01-01') AS localccy_forecast_202601_loss,
  SUM(loss) FILTER (WHERE metric = 'euws' AND forecast_date = DATE '2026-01-01') AS euws_202601_loss,
  SUM(loss) FILTER (WHERE metric = 'euws_override' AND forecast_date = DATE '2026-01-01') AS final_euws_override_202601_loss,

  SUM(loss) FILTER (WHERE metric = 'localccy_forecast' AND forecast_date = DATE '2026-07-01') AS localccy_forecast_202607_loss,
  SUM(loss) FILTER (WHERE metric = 'euws' AND forecast_date = DATE '2026-07-01') AS euws_202607_loss,
  SUM(loss) FILTER (WHERE metric = 'euws_override' AND forecast_date = DATE '2026-07-01') AS final_euws_override_202607_loss,

  SUM(loss) FILTER (WHERE metric = 'localccy_forecast' AND forecast_date = DATE '2026-12-31') AS localccy_forecast_202612_loss,
  SUM(loss) FILTER (WHERE metric = 'euws' AND forecast_date = DATE '2026-12-31') AS euws_202612_loss,
  SUM(loss) FILTER (WHERE metric = 'euws_override' AND forecast_date = DATE '2026-12-31') AS final_euws_override_202612_loss
FROM mts_tbl_ylt_combined_all_factors
WHERE rollup_lob = 'CHANGE_ME'
  AND rollup_peril = 'CHANGE_ME'
GROUP BY 1, 2, 3
ORDER BY final_euws_override_202601_loss DESC NULLS LAST;

-- Row-per-forecast metric waterfall, useful when checking row counts by stage.
SELECT
  forecast_date,
  base_model,
  rollup_lob,
  rollup_peril,
  metric,
  COUNT(*) AS rows,
  SUM(loss) AS loss
FROM mts_tbl_ylt_combined_all_factors
WHERE rollup_lob = 'CHANGE_ME'
  AND rollup_peril = 'CHANGE_ME'
  -- AND forecast_date = DATE '2026-01-01'
GROUP BY 1, 2, 3, 4, 5
ORDER BY
  forecast_date,
  base_model,
  CASE metric
    WHEN 'original' THEN 10
    WHEN 'blended' THEN 20
    WHEN 'localccy' THEN 30
    WHEN 'localccy_forecast' THEN 40
    WHEN 'euws' THEN 50
    WHEN 'euws_override' THEN 60
    ELSE 999
  END;

-- Largest event-level rows for the same LOB/peril, ordered by transform stage.
SELECT
  forecast_date,
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
  metric,
  loss,
  uplift_factor_on_base_model,
  risklink_blended_contribution,
  verisk_blended_contribution
FROM mts_tbl_ylt_combined_all_factors
WHERE rollup_lob = 'CHANGE_ME'
  AND rollup_peril = 'CHANGE_ME'
  -- AND forecast_date = DATE '2026-01-01'
ORDER BY
  year_id,
  event_id,
  forecast_date,
  CASE metric
    WHEN 'original' THEN 10
    WHEN 'blended' THEN 20
    WHEN 'localccy' THEN 30
    WHEN 'localccy_forecast' THEN 40
    WHEN 'euws' THEN 50
    WHEN 'euws_override' THEN 60
    ELSE 999
  END
LIMIT 500;
