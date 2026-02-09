{{ config(materialized='view') }}

with modelled_lobs as (
    select
        modelled_lob,
        rollup_lob,
        lob_type,
        cds_cat_class_name,
        id
    from {{ source('reference', 'lobs') }}
),
region_perils as (
    select
        vendor,
        modelled_region_peril,
        cleaned_region_peril,
        rollup_region_peril,
        region,
        peril,
        adjustments,
        applies_to_mga,
        applies_to_prop,
        applies_to_fa,
        blending_factor_region_peril_id,
        blending_factor_sub_region_peril_id,
        id
    from {{ source('core', 'dim_region_perils') }}
),
risklink as (
    select
        'risklink' as vendor,
        ep.rp,
        ep.ep_type,
        lobs.modelled_lob,
        lobs.rollup_lob,
        lobs.lob_type,
        rp.modelled_region_peril,
        rp.cleaned_region_peril,
        rp.rollup_region_peril,
        rp.region,
        rp.peril,
        rp.adjustments,
        case
            when lobs.lob_type = 'mga' then rp.applies_to_mga
            when lobs.lob_type = 'prop' then rp.applies_to_prop
            when lobs.lob_type = 'fa' then rp.applies_to_fa
            else 0
        end as official_rollup,
        rp.id as region_peril_id,
        lobs.id as lob_id,
        rp.blending_factor_region_peril_id,
        rp.blending_factor_sub_region_peril_id,
        lobs.cds_cat_class_name,
        ep.gl
    from {{ ref('stg_risklink__ep') }} as ep
    inner join modelled_lobs as lobs
        on lobs.modelled_lob = ep.lob
    inner join region_perils as rp
        on rp.modelled_region_peril = ep.region_peril
        and rp.vendor = 'risklink'
),
verisk as (
    select
        'verisk' as vendor,
        ep.rp,
        ep.ep_type,
        lobs.modelled_lob,
        lobs.rollup_lob,
        lobs.lob_type,
        rp.modelled_region_peril,
        rp.cleaned_region_peril,
        rp.rollup_region_peril,
        rp.region,
        rp.peril,
        rp.adjustments,
        case
            when lobs.lob_type = 'mga' then rp.applies_to_mga
            when lobs.lob_type = 'prop' then rp.applies_to_prop
            when lobs.lob_type = 'fa' then rp.applies_to_fa
            else 0
        end as official_rollup,
        rp.id as region_peril_id,
        lobs.id as lob_id,
        rp.blending_factor_region_peril_id,
        rp.blending_factor_sub_region_peril_id,
        lobs.cds_cat_class_name,
        ep.gl
    from {{ ref('stg_verisk__ep') }} as ep
    inner join modelled_lobs as lobs
        on lobs.modelled_lob = ep.lob
    inner join region_perils as rp
        on rp.modelled_region_peril = ep.analysis
        and rp.vendor = 'verisk'
)

select * from risklink
union all
select * from verisk
