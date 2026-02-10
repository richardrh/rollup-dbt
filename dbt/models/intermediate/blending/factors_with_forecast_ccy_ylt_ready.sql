{{ config(materialized='view') }}

select
    *
from {{ ref('factors_with_forecast_ccy') }}
where
    official_rollup = 1
    and ep_type in ('AAL', 'OEP')
