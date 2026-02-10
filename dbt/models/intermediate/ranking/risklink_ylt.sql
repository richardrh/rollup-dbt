{{ config(materialized='view') }}

select
    lobs.id as lob_id,
    lobs.modelled_lob,
    lobs.rollup_lob,
    lobs.lob_type,
    lobs.cds_cat_class_name,
    dra.rl_analysis_id,
    rps.id as region_peril_id,
    rps.modelled_region_peril,
    rps.cleaned_region_peril,
    rps.rollup_region_peril,
    ylt.yearid,
    ylt.eventid,
    ylt.loss
from {{ ref('stg_risklink__ylts') }} as ylt
inner join {{ source('core', 'dim_rl_analysis') }} as dra
    on dra.rl_analysis_id = ylt.anlsid
inner join {{ source('core', 'dim_region_perils') }} as rps
    on rps.modelled_region_peril = dra.region_peril
inner join {{ source('reference', 'lobs') }} as lobs
    on lobs.modelled_lob = dra.lob
