{{ config(materialized='view') }}

with blending as (
    select * from {{ ref('int_blending') }}
),

unpivoted as (
    select
        aggregation_key,
        run_date,
        base_date,
        office,
        class,
        cleaned_region_peril,
        ep_type,
        return_period,
        rank_num,
        base_vendor,
        'risklink_annual_loss' as metric_name,
        risklink_annual_loss as metric_value
    from blending
    where risklink_annual_loss is not null
    
    union all
    
    select
        aggregation_key,
        run_date,
        base_date,
        office,
        class,
        cleaned_region_peril,
        ep_type,
        return_period,
        rank_num,
        base_vendor,
        'verisk_annual_loss' as metric_name,
        verisk_annual_loss as metric_value
    from blending
    where verisk_annual_loss is not null
    
    union all
    
    select
        aggregation_key,
        run_date,
        base_date,
        office,
        class,
        cleaned_region_peril,
        ep_type,
        return_period,
        rank_num,
        base_vendor,
        'blended_annual_loss' as metric_name,
        blended_annual_loss as metric_value
    from blending
    where blended_annual_loss is not null
)

select * from unpivoted
