{{ config(materialized='view') }}

-- This brings together the two calculated EPs for both models
-- so that they can be compared
with modelled_lobs as (
    select
        modelled_lob,
        rollup_lob,
        lob_type,
        cds_cat_class_name,
        id
    from {{ source('reference', 'lobs') }}
),
region_perils as (
    select
        vendor,
        modelled_region_peril,
        cleaned_region_peril,
        rollup_region_peril,
        region,
        peril,
        adjustments,
        applies_to_mga,
        applies_to_prop,
        applies_to_fa,
        blending_factor_region_peril_id,
        blending_factor_sub_region_peril_id,
        id
    from {{ source('core', 'dim_region_perils') }}
),

with risklink_ep as (
select
    anlsid,
    yearid,
    eventid,
    loss
from {{ source('staging', 'stg_risklink__ylt.sql') }}
