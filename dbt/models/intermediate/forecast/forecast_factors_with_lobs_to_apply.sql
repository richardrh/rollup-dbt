{{ config(materialized='view') }}

select
    lob_id,
    max(case when forecast_date = '202601' then factor end) as f_202601,
    max(case when forecast_date = '202607' then factor end) as f_202607,
    max(case when forecast_date = '202701' then factor end) as f_202701,
    max(office) as office,
    max("class") as "class"
from {{ ref('forecast_factors') }}
group by
    lob_id
