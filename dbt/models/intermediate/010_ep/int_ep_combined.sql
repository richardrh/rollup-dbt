{{ config(
    materialized='incremental',
    unique_key=['aggregation_key', 'source_vendor', 'ep_type', 'return_period', 'rank_num'],
    incremental_strategy='delete+insert'
) }}

/*
    Incremental EP curve calculation for both vendors.
    Only processes new/changed data on incremental runs.
    
    This query calculates EP curve for both vendors.
    First split them out as they have different n_sims
    calc ep curve for both, union the result together.
    Then join back to dim distinct ylts which contains the aggregation keys.
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

-- Verisk EP calculation (10K simulations)
verisk_target_return_periods as (
    select unnest([1000, 500, 250, 200, 150, 100, 50, 30, 20, 10, 5]) as return_period
),

verisk_annual_losses as (
    select
        aggregation_key,
        year_id,
        sum(loss) as annual_loss
    from ylt_data
    where source_vendor = 'verisk'
    group by aggregation_key, year_id
),

verisk_max_losses as (
    select
        aggregation_key,
        year_id,
        max(loss) as annual_loss
    from ylt_data
    where source_vendor = 'verisk'
    group by aggregation_key, year_id
),

verisk_combined_losses as (
    select 'AEP' as ep_type, * from verisk_annual_losses
    union all
    select 'OEP' as ep_type, * from verisk_max_losses
),

verisk_ranked_losses as (
    select
        aggregation_key,
        ep_type,
        annual_loss,
        row_number() over (
            partition by aggregation_key, ep_type
            order by annual_loss desc
        ) as rank_num
    from verisk_combined_losses
),

verisk_ep_curve as (
    select
        r.aggregation_key,
        r.ep_type,
        rp.return_period,
        r.rank_num,
        r.annual_loss
    from verisk_ranked_losses r
    inner join verisk_target_return_periods rp
        on rp.return_period = floor(10000::float / r.rank_num)
),

verisk_aal as (
    select
        aggregation_key,
        'AAL' as ep_type,
        0 as return_period,
        0 as rank_num,
        sum(loss) / 10000::float as annual_loss
    from ylt_data
    where source_vendor = 'verisk'
    group by aggregation_key
),

-- RiskLink EP calculation (100K simulations)
risklink_target_return_periods as (
    select unnest([1000, 500, 250, 200, 150, 100, 50, 30, 20, 10, 5]) as return_period
),

risklink_annual_losses as (
    select
        aggregation_key,
        year_id,
        sum(loss) as annual_loss
    from ylt_data
    where source_vendor = 'risklink'
    group by aggregation_key, year_id
),

risklink_max_losses as (
    select
        aggregation_key,
        year_id,
        max(loss) as annual_loss
    from ylt_data
    where source_vendor = 'risklink'
    group by aggregation_key, year_id
),

risklink_combined_losses as (
    select 'AEP' as ep_type, * from risklink_annual_losses
    union all
    select 'OEP' as ep_type, * from risklink_max_losses
),

risklink_ranked_losses as (
    select
        aggregation_key,
        ep_type,
        annual_loss,
        row_number() over (
            partition by aggregation_key, ep_type
            order by annual_loss desc
        ) as rank_num
    from risklink_combined_losses
),

risklink_ep_curve as (
    select
        r.aggregation_key,
        r.ep_type,
        rp.return_period,
        r.rank_num,
        r.annual_loss
    from risklink_ranked_losses r
    inner join risklink_target_return_periods rp
        on rp.return_period = floor(100000::float / r.rank_num)
),

risklink_aal as (
    select
        aggregation_key,
        'AAL' as ep_type,
        0 as return_period,
        0 as rank_num,
        sum(loss) / 100000::float as annual_loss
    from ylt_data
    where source_vendor = 'risklink'
    group by aggregation_key
),

-- Combine all EP curves
ep_curves as (
    select aggregation_key, ep_type, return_period, rank_num, annual_loss from verisk_ep_curve
    union all
    select aggregation_key, ep_type, return_period, rank_num, annual_loss from verisk_aal
    union all
    select aggregation_key, ep_type, return_period, rank_num, annual_loss from risklink_ep_curve
    union all
    select aggregation_key, ep_type, return_period, rank_num, annual_loss from risklink_aal
),

dim_lookup as (
    select *
    from {{ ref('int_setup__dim_distinct_ylts') }}
    {% if is_incremental() %}
    where aggregation_key in (select aggregation_key from ylt_data)
    {% endif %}
),

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
