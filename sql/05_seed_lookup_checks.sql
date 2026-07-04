-- Exported seed lookup checks.

-- LOB mappings.
SELECT
  rollup_lob,
  modelled_lob,
  lob_type,
  office,
  class,
  currency
FROM seed_lobs
ORDER BY rollup_lob, modelled_lob;

-- Peril selection flags for main, DIALSUP, and EUWS logic.
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

-- FX rates used. Rates are local currency to GBP; output conversion inverts them.
SELECT *
FROM seed_fx_rates
ORDER BY currency_code, rate_date;

-- Forecast factors by month.
SELECT
  forecast_date,
  COUNT(*) AS rows,
  MIN(factor) AS min_factor,
  MAX(factor) AS max_factor
FROM seed_forecast_factors
GROUP BY 1
ORDER BY forecast_date;
