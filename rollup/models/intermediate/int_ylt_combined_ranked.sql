{{ config(materialized='view') }}

with ylt as (
    select
        'verisk' as vendor,
        lob_id,
        region_peril_id,
        rollup_region_peril,
        rollup_lob,
        cds_cat_class_name,
        model_code,
        yearid,
        eventid,
        loss
    from {{ ref('int_verisk_ylt') }}

    union all

    select
        'risklink' as vendor,
        lob_id,
        region_peril_id,
        rollup_region_peril,
        rollup_lob,
        cds_cat_class_name,
        0 as model_code,
        yearid,
        eventid,
        loss
    from {{ ref('int_risklink_ylt') }}
)

select
    row_number() over (
        partition by vendor, lob_id, region_peril_id
        order by loss desc
    ) as rnk,
    vendor,
    lob_id,
    region_peril_id,
    rollup_region_peril,
    rollup_lob,
    cds_cat_class_name,
    model_code,
    yearid,
    eventid,
    loss
from ylt
