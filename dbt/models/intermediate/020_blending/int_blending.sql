{{ config(materialized='view') }}

/*
    Blending funnel for YLT data across all vendors.
    
    Uses wide format (explicit vendor joins) for readable blending calculations.
    This model will be unpivoted back to long format in a subsequent model.
*/

with ep_curves as (
    select *
    from {{ ref('int_ep_combined') }}
),

analysis_lookup as (
    select *
    from {{ ref('stg_cat_modelling_results__analysis_lists') }}
),

blending_factors as (
    select *
    from {{ ref('stg_reference__vor_blending_factors') }}
),

-- Join EP curves with analysis details
ep_with_details as (
    select
        ep.aggregation_key,
        ep.run_date,
        ep.source_vendor,
        ep.analysis_id,
        ep.ep_type,
        ep.return_period,
        ep.rank_num,
        ep.annual_loss,
        lkp.modelled_lob,
        lkp.modelled_peril
    from ep_curves ep
    inner join analysis_lookup lkp
        on lkp.analysis_id = ep.analysis_id
),

-- Split into vendor-specific CTEs
verisk_ep as (
    select
        aggregation_key,
        run_date,
        modelled_lob,
        modelled_peril,
        ep_type,
        return_period,
        rank_num,
        annual_loss as verisk_annual_loss
    from ep_with_details
    where source_vendor = 'verisk'
),

risklink_ep as (
    select
        aggregation_key,
        run_date,
        modelled_lob,
        modelled_peril,
        ep_type,
        return_period,
        rank_num,
        annual_loss as risklink_annual_loss
    from ep_with_details
    where source_vendor = 'risklink'
),

-- Wide format join with explicit column aliasing
ep_wide as (
    select
        -- Dimensions (from risklink, they're the same in both)
        coalesce(r.aggregation_key, v.aggregation_key) as aggregation_key,
        coalesce(r.run_date, v.run_date) as run_date,
        coalesce(r.modelled_lob, v.modelled_lob) as modelled_lob,
        coalesce(r.modelled_peril, v.modelled_peril) as modelled_peril,
        coalesce(r.ep_type, v.ep_type) as ep_type,
        coalesce(r.return_period, v.return_period) as return_period,
        coalesce(r.rank_num, v.rank_num) as rank_num,
        
        -- RiskLink values (may be null if no matching Verisk data)
        r.risklink_annual_loss,
        
        -- Verisk values (may be null if no matching RiskLink data)  
        v.verisk_annual_loss,
        
        -- Blending factors
        bf.air_blend,
        bf.rms_blend
        
    from risklink_ep r
    full outer join verisk_ep v
        on v.aggregation_key = r.aggregation_key
        and v.ep_type = r.ep_type
        and v.return_period = r.return_period
        and v.rank_num = r.rank_num
    left join blending_factors bf
        on bf.sub_region_peril = coalesce(r.modelled_peril, v.modelled_peril)
)

select 
    aggregation_key,
    run_date,
    modelled_lob,
    modelled_peril,
    ep_type,
    return_period,
    rank_num,
    
    -- Individual vendor losses
    risklink_annual_loss,
    verisk_annual_loss,
    
    -- Blending factors
    air_blend,
    rms_blend,
    
    -- Blended loss calculation (readable in wide format)
    (
        coalesce(risklink_annual_loss, 0) * coalesce(rms_blend, 0) +
        coalesce(verisk_annual_loss, 0) * coalesce(air_blend, 0)
    ) / nullif(coalesce(rms_blend, 0) + coalesce(air_blend, 0), 0) 
        as blended_annual_loss
        
from ep_wide
