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

-- Core table row counts. These are expected in a normal run.
SELECT 'ep_report' AS table_name, COUNT(*) AS rows
FROM ep_report
UNION ALL
SELECT 'mts_tbl_ylt_combined_all_factors', COUNT(*)
FROM mts_tbl_ylt_combined_all_factors
UNION ALL
SELECT 'mts_tbl_ylt_combined_all_factors_wide', COUNT(*)
FROM mts_tbl_ylt_combined_all_factors_wide
UNION ALL
SELECT 'mts_tbl_ylt_dialsup', COUNT(*)
FROM mts_tbl_ylt_dialsup
ORDER BY table_name;

-- Generated wide loss columns. Use this before editing wide waterfall templates.
SELECT
  column_name
FROM information_schema.columns
WHERE table_schema = 'main'
  AND table_name = 'mts_tbl_ylt_combined_all_factors_wide'
  AND column_name LIKE '%\_loss' ESCAPE '\'
ORDER BY ordinal_position;

-- Exported seed tables.
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'main'
  AND table_name LIKE 'seed\_%' ESCAPE '\'
ORDER BY table_name;
