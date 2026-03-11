{{config(MATERIALIZED ='view',
schema = 'raw') }}SELECT
  date,
  vendor as source,
  type,
  filename,
  analysis_id,
  modelled_lob,
  modelled_peril as region_peril,
  is_official
FROM
  read_parquet(
    '{{ var("cat_results_path") }}/date=*/vendor=*/type=analysis_list/*.parquet',
    hive_partitioning = TRUE,
    union_by_name = TRUE,
    filename = TRUE
  )
