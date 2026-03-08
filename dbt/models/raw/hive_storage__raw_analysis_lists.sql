{{config(MATERIALIZED ='view',
schema = 'raw') }}SELECT
  DATE,
  source,
  type,
  filename,
  analysis_id,
  modelled_lob,
  region_peril,
  analysis_modifications,
  is_official,
  AAL,
  OEP_200,
  OEP_1000,
  AEP_200,
  AEP_1000
FROM
  read_parquet(
    '{{ var("cat_results_path") }}/date=*/source=*/type=analysis_list/*.parquet',
    hive_partitioning = TRUE,
    union_by_name = TRUE,
    filename = TRUE
  )
