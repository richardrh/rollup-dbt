-- Example CSV export pattern.
-- Change the SELECT and destination path for the extract you need.

COPY (
  SELECT *
  FROM mts_tbl_ylt_combined_all_factors
  WHERE metric = 'euws_override'
  LIMIT 100
) TO 'output/analysis/example_extract.csv' (HEADER, DELIMITER ',');
