{{ config(materialized='view') }}

with filtered_ep as (
    select
        vendor,
        lob_id,
        modelled_lob,
        rollup_lob,
        lob_type,
        region_peril_id,
        modelled_region_peril,
        rollup_region_peril,
        adjustments,
        official_rollup,
        ep_type,
        rp,
        blending_factor_region_peril_id,
        blending_factor_sub_region_peril_id,
        cds_cat_class_name,
        round(gl, 0) as gl
from {{ ref('ep_union') }}
    where
        official_rollup = 1
        and ep_type in ('AAL', 'OEP')
        and rp in (0, 200, 1000, 10000)
    order by rollup_region_peril
),
risklink as (
    select *
    from filtered_ep
    where vendor = 'risklink'
),
verisk as (
    select *
    from filtered_ep
    where vendor = 'verisk'
)
select
    risklink.lob_id,
    risklink.modelled_lob,
    risklink.rollup_lob,
    risklink.lob_type,
    risklink.region_peril_id as risklink_region_peril_id,
    verisk.region_peril_id as verisk_region_peril_id,
    risklink.modelled_region_peril,
    risklink.rollup_region_peril,
    risklink.adjustments,
    risklink.official_rollup,
    risklink.blending_factor_region_peril_id,
    risklink.blending_factor_sub_region_peril_id,
    risklink.cds_cat_class_name,
    risklink.ep_type,
    risklink.rp,
    risklink.gl as risklink_loss,
    verisk.gl as verisk_loss,
    (risklink.gl / (risklink.gl + verisk.gl)) as risklink_proportion,
    (verisk.gl / (risklink.gl + verisk.gl)) as verisk_proportion,
    (risklink.gl / verisk.gl) as ratio_to_verisk
from risklink
inner join verisk
    on risklink.rollup_lob = verisk.rollup_lob
    and risklink.rollup_region_peril = verisk.rollup_region_peril
    and risklink.ep_type = verisk.ep_type
    and risklink.rp = verisk.rp
