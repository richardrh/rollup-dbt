{{ config(materialized='view') }}

with blended_long as (
    select * from {{ ref('int_blending_long') }}
),

forecast_factors as (
    select * from {{ ref('stg_reference__hisco_org__forecast_factors') }}
),

forecasted as (
    select
        b.aggregation_key,
        b.run_date,
        b.base_date,
        f.forecast_date,
        b.office,
        b.class,
        b.cleaned_region_peril,
        b.ep_type,
        b.return_period,
        b.rank_num,
        b.metric_name,
        b.metric_value as original_value,
        f.forecast_factor,
        b.metric_value * coalesce(f.forecast_factor, 1) as forecasted_value,
        b.base_vendor,
        case
            when b.metric_name like '%' || b.base_vendor || '%' then true
            else false
        end as is_base_model
    from blended_long b
    left join forecast_factors f
        on f.class = b.class
        and f.office = b.office
        and f.base_date = b.base_date
)

select * from forecasted
