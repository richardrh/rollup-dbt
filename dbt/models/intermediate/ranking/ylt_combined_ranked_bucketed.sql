{{ config(materialized='view') }}

select
    case
        when vendor = 'risklink' then cast((100000.0 / rnk) as integer)
        when vendor = 'verisk' then cast((10000.0 / rnk) as integer)
        else null
    end as rp,
    case
        when rp < 200 then 0
        when rp >= 200 and rp < 1000 then 200
        when rp >= 1000 and rp < 10000 then 1000
        when rp >= 10000 then 10000
        else null
    end as rp_bucket,
    *
from {{ ref('ylt_combined_ranked') }}
order by
    vendor,
    lob_id,
    region_peril_id,
    rnk
