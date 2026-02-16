{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('vor__blending_factors') }}
),


renamed as (
    select

        {{ dbt_utils.generate_surrogate_key([

            'blending_factor_id',
            'BlendSetID',
            'RegionPerilID',
            'SubRegionPerilID'

        ]) }} as blending_factor_id,


        blending_factor_id as original_hiscox_blending_factor_id,
        BlendSetID as blend_set_id,
        RegionPerilID as blend_region_peril_id,
        SubRegionPerilID as blend_sub_region_peril_id,
        SubRegionPeril as sub_region_peril,
        AIRBlend as verisk_blend_factor,
        RMSBlend as risklink_blend_factor,
        created_at

    from source
)

select * from renamed
