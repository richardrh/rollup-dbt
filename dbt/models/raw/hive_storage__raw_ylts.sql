{{ config(materialized='view', schema='raw') }}

select
    date,
    vendor,
    filename,
    analysis_id,
    model_id,
    year_id,
    event_id,
    loss
from read_parquet(
    '{{ var("cat_results_path") }}/date=*/vendor=*/type=ylt/*.parquet',
    hive_partitioning = true,
    filename = true,
    union_by_name = true
)
