{{ config(materialized='view') }}

select
    lobs.id as lob_id,
    lobs.modelled_lob,
    lobs.rollup_lob,
    lobs.lob_type,
    lobs.cds_cat_class_name,
    rps.id as region_peril_id,
    rps.modelled_region_peril,
    rps.cleaned_region_peril,
    rps.rollup_region_peril,
    ylt.model_code,
    ylt.yearid,
    ylt.eventid,
    ylt.net_pre_cat_loss as loss
from {{ ref('stg_verisk__ylt') }} as ylt
inner join {{ source('reference', 'lobs') }} as lobs
    on lobs.modelled_lob = ylt.lob
inner join {{ source('core', 'dim_region_perils') }} as rps
    on rps.modelled_region_peril = ylt.analysis
where
    rps.vendor = 'verisk'
    and ylt.catalog_type_code = 'STC'
