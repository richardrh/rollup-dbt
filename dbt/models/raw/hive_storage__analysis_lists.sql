{{ config(materialized='view', schema='raw') }}

select
    date,
    vendor,
    analysis_id,
    modelled_lob,
    modelled_peril,
    is_official

from read_parquet(
    '{{ var("cat_results_path") }}/date=*/vendor=*/type=analysis_list/*.parquet',
    hive_partitioning = true
)
