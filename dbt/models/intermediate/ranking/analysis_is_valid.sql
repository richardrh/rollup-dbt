{{ config(materialized='view') }}

select
    lob_id,
    region_peril_id,
    max(official_rollup) as official_rollup
from {{ ref('ep_union') }}
group by
    lob_id,
    region_peril_id
