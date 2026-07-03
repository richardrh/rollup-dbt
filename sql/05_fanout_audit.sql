-- CDS fanout audit queries.

-- Fanout row counts and loss totals.
SELECT
  fanout_name,
  forecast_yyyymm,
  fanout_metric,
  COUNT(*) AS rows,
  SUM(ModelGrossLoss) AS gross_loss
FROM cds_fanouts
GROUP BY 1, 2, 3
ORDER BY forecast_yyyymm, fanout_name, fanout_metric;

-- Largest fanout events.
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
