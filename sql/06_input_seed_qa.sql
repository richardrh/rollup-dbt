-- Input and seed QA queries.

-- EP summary coverage.
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

-- Seed peril selection flags.
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
