{{ config(materialized='view') }}

select
    rp,
    ep_type,
    lob,
    analysis,
    gl
from {{ source('catmodel', 'verisk_ep') }}
