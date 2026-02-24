{{ config(materialized='view') }}

select
    anlsid,
    yearid,
    eventid,
    loss
from {{ source('cat_models_raw', 'risklink__ylts') }}
