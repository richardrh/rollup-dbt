{{ config(materialized='view') }}

select
    rp,
    ep_type,
    lob,
    region_peril,
    gl
from {{ source('catmodel', 'risklink_ep') }}
