-- EP report inspection queries for output/rollup.duckdb.

-- Row counts by metric and EP type.
SELECT
  forecast_date,
  metric,
  ep_type,
  COUNT(*) AS rows,
  SUM(loss) AS total_loss
FROM ep_report
GROUP BY 1, 2, 3
ORDER BY forecast_date, metric, ep_type;

-- EP losses by portfolio dimension and return period.
SELECT
  forecast_date,
  metric,
  base_model,
  rollup_lob,
  rollup_peril,
  ep_type,
  return_period,
  loss
FROM ep_report
ORDER BY forecast_date, metric, rollup_lob, rollup_peril, ep_type, return_period;

-- Largest reported EP losses.
SELECT
  forecast_date,
  metric,
  base_model,
  rollup_lob,
  rollup_peril,
  ep_type,
  return_period,
  rank,
  rp,
  loss
FROM ep_report
ORDER BY loss DESC
LIMIT 100;
