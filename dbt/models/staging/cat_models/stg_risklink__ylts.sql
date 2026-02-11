{{ config(materialized='view') }}

select
    anlsid,
    yearid,
    eventid,
    loss
from {{ source('cat_models_raw', 'stg_risklink__ylts') }}
