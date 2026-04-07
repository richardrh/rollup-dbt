{{ config(materialized='view') }}

with blended_ep as (
    select * from {{ ref('int_blending') }}
),

dim as (
    select * from {{ ref('int_setup__dim_distinct_ylts') }}
),

enriched as (
    select
        b.aggregation_key,
        b.run_date,
        d.base_date,
        d.office,
        d.class,
        b.modelled_lob,
        b.modelled_peril,
        b.ep_type,
        b.return_period,
        b.rank_num,
        b.risklink_annual_loss,
        b.verisk_annual_loss,
        b.air_blend,
        b.rms_blend,
        b.blended_annual_loss
    from blended_ep b
    inner join dim d on d.aggregation_key = b.aggregation_key
)

select * from enriched
