{{ config(materialized='view') }}

with mock_blending_data as (
    select
        'blend_001' as blending_factor_id,
        1 as id,
        'SET_001' as blend_set_id,
        'REG_001' as region_peril_id,
        'US Hurricane' as region_peril,
        'SUB_001' as sub_region_peril_id,
        'US_Hurricane' as sub_region_peril,
        0.6 as air_blend,
        0.4 as rms_blend
    
    union all
    
    select
        'blend_002' as blending_factor_id,
        2 as id,
        'SET_001' as blend_set_id,
        'REG_002' as region_peril_id,
        'US Earthquake' as region_peril,
        'SUB_002' as sub_region_peril_id,
        'US_Earthquake' as sub_region_peril,
        0.7 as air_blend,
        0.3 as rms_blend
)

select * from mock_blending_data
