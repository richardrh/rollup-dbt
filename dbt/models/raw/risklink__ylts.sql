{{ config(materialized='view', schema='raw') }}

select
    date,
    filename,
    analysis_id,
    year_id,
    event_id,
    -- TODO: need to add perspective here maybe? Could also just create a new anlsid for it
    loss
from read_parquet('{{ var("cat_results_path") }}/risklink/ylts/**/*.parquet',
    hive_partitioning=true,
    filename=true)
