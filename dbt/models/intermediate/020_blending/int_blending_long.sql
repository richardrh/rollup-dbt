{{ config(materialized='view') }}

with enriched as (
    select * from {{ ref('int_blending_enriched') }}
),

unpivoted as (
    select
        aggregation_key,
        run_date,
        base_date,
        office,
        class,
        modelled_lob,
        modelled_peril,
        ep_type,
        return_period,
        rank_num,
        air_blend,
        rms_blend,
        'risklink_annual_loss' as metric_name,
        risklink_annual_loss as metric_value
    from enriched
    where risklink_annual_loss is not null
    
    union all
    
    select
        aggregation_key,
        run_date,
        base_date,
        office,
        class,
        modelled_lob,
        modelled_peril,
        ep_type,
        return_period,
        rank_num,
        air_blend,
        rms_blend,
        'verisk_annual_loss' as metric_name,
        verisk_annual_loss as metric_value
    from enriched
    where verisk_annual_loss is not null
    
    union all
    
    select
        aggregation_key,
        run_date,
        base_date,
        office,
        class,
        modelled_lob,
        modelled_peril,
        ep_type,
        return_period,
        rank_num,
        air_blend,
        rms_blend,
        'blended_annual_loss' as metric_name,
        blended_annual_loss as metric_value
    from enriched
    where blended_annual_loss is not null
)

select * from unpivoted
