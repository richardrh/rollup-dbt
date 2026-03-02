{{ config(materialized='view', schema='raw') }}

select
    date,
    regexp_extract(filename, 'cat_results/([^/]+)/ylts/', 1) as vendor,
    filename,
    sha256(concat_ws('||',
        date,
        regexp_extract(filename, 'cat_results/([^/]+)/ylts/', 1),
        analysis_id,
        year_id,
        event_id,
        cast(loss as text)
    )) as content_hash,
    analysis_id,
    year_id,
    event_id,
    loss
from read_parquet('{{ var("cat_results_path") }}/*/*.parquet',
    hive_partitioning=true,
    filename=true)
