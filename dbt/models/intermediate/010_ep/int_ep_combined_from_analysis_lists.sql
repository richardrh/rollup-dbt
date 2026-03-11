{{ config(materialized='view') }}

with source as (
    select * from {{ ref('stg_cat_modelling_results__analysis_lists') }}
),

lobs as (
    select
        modelled_lob,
        rollup_lob
    from {{ ref('stg_reference__hisco_org__lobs') }}
    group by 1, 2
),

with_rollup as (
    select
        s.source_vendor,
        s.analysis_id,
        s.modelled_lob,
        l.rollup_lob,
        s.region_peril as peril,
        s.is_official,
        s.AAL,
        s.OEP_200,
        s.OEP_1000,
        s.AEP_200,
        s.AEP_1000
    from source s
    left join lobs l on s.modelled_lob = l.modelled_lob
),

risklink as (
    select
        modelled_lob,
        rollup_lob,
        peril,
        AAL as risklink_aal,
        OEP_200 as risklink_oep_200,
        OEP_1000 as risklink_oep_1000,
        AEP_200 as risklink_aep_200,
        AEP_1000 as risklink_aep_1000
    from with_rollup
    where source_vendor = 'risklink'
),

verisk as (
    select
        modelled_lob,
        rollup_lob,
        peril,
        AAL as verisk_aal,
        OEP_200 as verisk_oep_200,
        OEP_1000 as verisk_oep_1000,
        AEP_200 as verisk_aep_200,
        AEP_1000 as verisk_aep_1000
    from with_rollup
    where source_vendor = 'verisk'
)

select
    r.modelled_lob,
    r.rollup_lob,
    r.peril,
    r.risklink_aal,
    r.risklink_oep_200,
    r.risklink_oep_1000,
    r.risklink_aep_200,
    r.risklink_aep_1000,
    v.verisk_aal,
    v.verisk_oep_200,
    v.verisk_oep_1000,
    v.verisk_aep_200,
    v.verisk_aep_1000
from risklink r
left join verisk v 
    on r.modelled_lob = v.modelled_lob
    and r.rollup_lob = v.rollup_lob
    and r.peril = v.peril
