{{ config(materialized='view') }}

select
    rp,
    ep_type,
    lob,
    analysis,
    gl
from {{ source('cat_models_raw', 'stg_verisk__eps') }}
