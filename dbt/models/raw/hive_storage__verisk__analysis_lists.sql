{{ config(materialized='view', schema='raw') }}

select
    date,
    regexp_extract(filename, 'cat_results/([^/]+)/ylts/', 1) as vendor,
    filename,
    analysis_id,
    year_id,
    event_id,
    loss
from read_parquet('{{ var("cat_results_path") }}/*/*.parquet',
    hive_partitioning=true,
    filename=true)
