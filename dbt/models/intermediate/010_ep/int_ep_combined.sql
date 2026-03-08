{{ config(
    materialized='incremental',
    unique_key=['aggregation_key', 'source_vendor', 'ep_type', 'return_period', 'rank_num'],
    incremental_strategy='delete+insert'
) }}

/*
    Incremental EP curve calculation for both vendors using ep_curve_from_ylt macro.
    Only processes new/changed data on incremental runs.
*/

with ylt_data as (
    select *
    from {{ ref('stg_cat_modelling_results__ylts') }}
    
    {% if is_incremental() %}
    where aggregation_key in (
        select distinct aggregation_key 
        from {{ ref('stg_cat_modelling_results__ylts') }}
        where run_date > (
            select coalesce(max(run_date), '1900-01-01'::date) 
            from {{ this }}
        )
    )
    or aggregation_key not in (
        select aggregation_key from {{ this }}
    )
    {% endif %}
),

-- Verisk EP calculation using macro (10K simulations)
verisk_ep as (
    {{ ep_curve_from_ylt('ylt_data', 'loss', 10000, 'aggregation_key') }}
),

-- RiskLink EP calculation using macro (100K simulations)
risklink_ep as (
    {{ ep_curve_from_ylt('ylt_data', 'loss', 100000, 'aggregation_key') }}
),

-- Combine both vendors' EP curves
ep_curves as (
    select aggregation_key, ep_type, return_period, rank_num, annual_loss from verisk_ep
    union all
    select aggregation_key, ep_type, return_period, rank_num, annual_loss from risklink_ep
),

-- Lookup dimension data (run_date, source_vendor, etc.)
dim_lookup as (
    select *
    from {{ ref('int_setup__dim_distinct_ylts') }}
    {% if is_incremental() %}
    where aggregation_key in (select aggregation_key from ylt_data)
    {% endif %}
),

-- Final result with dimension attributes
wrap_up as (
    select
        d.aggregation_key,
        d.run_date,
        d.source_vendor,
        d.source_file,
        d.analysis_id,
        ep.ep_type,
        ep.return_period,
        ep.rank_num,
        ep.annual_loss
    from ep_curves ep
    join dim_lookup d using (aggregation_key)
)

select * from wrap_up
