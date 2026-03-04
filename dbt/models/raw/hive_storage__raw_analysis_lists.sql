{{ config(materialized='view', schema='raw') }}

select
     date
    ,source
    ,type
    ,filename
    ,analysis_id
    ,modelled_lob
    ,region_peril
    ,analysis_modifications
    ,is_official
from read_parquet(
    '{{ var("cat_results_path") }}/date=*/source=*/type=analysis_list/*.parquet',
    hive_partitioning = true,
    union_by_name = true,
    filename = true
)
