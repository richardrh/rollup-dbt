-- Inventory queries for output/rollup.duckdb.

-- Tables in the export.
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'main'
ORDER BY table_name;

-- Column dictionary.
SELECT
  table_name,
  column_name,
  data_type
FROM information_schema.columns
WHERE table_schema = 'main'
ORDER BY table_name, ordinal_position;

-- Core table row counts.
SELECT 'mts_tbl_ylt_combined_all_factors' AS table_name, COUNT(*) AS rows
FROM mts_tbl_ylt_combined_all_factors
UNION ALL
SELECT 'mts_tbl_ylt_combined_all_factors_wide', COUNT(*)
FROM mts_tbl_ylt_combined_all_factors_wide
UNION ALL
SELECT 'mts_tbl_ylt_dialsup', COUNT(*)
FROM mts_tbl_ylt_dialsup
UNION ALL
SELECT 'cds_fanouts', COUNT(*)
FROM cds_fanouts
UNION ALL
SELECT 'input_ep_summaries', COUNT(*)
FROM input_ep_summaries
ORDER BY table_name;
