{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('vor_blending_factors') }}
),

renamed as (
    select

        {{ dbt_utils.generate_surrogate_key([
            'id',
            'BlendSetID',
            'SubRegionPerilID'
        ]) }} as blending_factor_id,

        id,
        BlendSetID      as blend_set_id,
        RegionPerilID   as region_peril_id,
        RegionPeril     as region_peril,
        SubRegionPerilID as sub_region_peril_id,
        SubRegionPeril  as sub_region_peril,
        AIRBlend        as air_blend,
        RMSBlend        as rms_blend

    from source
)

select * from renamed
