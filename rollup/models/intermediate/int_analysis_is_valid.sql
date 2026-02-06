{{ config(materialized='view') }}

select
    lob_id,
    region_peril_id,
    max(official_rollup) as official_rollup
from {{ ref('int_ep') }}
group by
    lob_id,
    region_peril_id
