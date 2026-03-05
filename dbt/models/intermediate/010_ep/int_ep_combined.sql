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

with source_ylt as (
    select 
      pk,
      aggregation_key,
      source_vendor,
      run_date,
      filename,
      analysis_id, 
      model_code,
      year_id,
      event_id,
      loss
    from {{ ref('stg_cat_modelling_results__ylts') }}
)

, dim_distinct_ylts as (



) 
, verisk_ylt as (
    select * from source_ylt where source_vendor = 'verisk'
)

, risklink_ylt as (
    select * from source_ylt where source_vendor = 'risklink'
)

, verisk_ep as (
    ep_curve_from_ylt(verisk_ylt, loss,
      10000, aggregation_key
    )
  )
)

, risklink_ep as (
  ep_curve_from_ylt(risklink_ylt, loss, 100000, aggregation)
)

