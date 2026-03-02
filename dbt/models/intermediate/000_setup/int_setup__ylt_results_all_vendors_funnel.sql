{{ config(materialized='view') }}

/*
    Setup funnel for YLT results from all vendors.

    Combines YLT data from all vendor-specific staging models
    to create a unified view of all YLT results. This model
    serves as the foundation for downstream vendor blending
    and aggregation logic.
*/

with ylts as (

    select
        aggregation_key,
        source_vendor,
        run_date,
        source_file,
        analysis_id,
        year_id,
        event_id,
        loss
    from {{ ref('stg_cat_modelling_results__ylts') }}

),

all_vendors as (
    select * from risklink_ylts
    -- Future: union all with verisk_ylts, etc.
)

select
    row_number() over (
        order by source_vendor, run_date, source_file, analysis_id, year_id, event_id
    ) as funnel_id,
    aggregation_key,
    source_vendor,
    run_date,
    source_file,
    analysis_id,
    year_id,
    event_id,
    loss
from all_vendors
