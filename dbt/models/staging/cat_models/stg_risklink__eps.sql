{{ config(materialized='view') }}

select
    rp,
    ep_type,
    lob,
    region_peril,
    gl
from {{ source('cat_models_raw', 'stg_risklink__eps') }}
