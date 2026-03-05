{{ config(materialized='table') }}

/*
    Dimension lookup for the RiskLink EP aggregation key.

    Provides one row per unique aggregation_key, mapping the opaque hash back
    to the human-readable dimension columns that were used to produce it.
    Downstream mart models join this on aggregation_key to recover labels.
    
    Enhanced to include region peril lookup and blending factors.
*/

select distinct
    ylt.aggregation_key,
    ylt.run_date,
    ylt.source_vendor,
    ylt.source_file,
    ylt.analysis_id,
    al.modelled_lob,
    al.region_peril,
    al.analysis_modifications,
    lobs.office,
    lobs.class,
    lobs.lob_type,
    date_trunc('month', ylt.run_date) as base_date,
    rp.cleaned_region_peril,
    rp.rollup_region_peril,
    rp.adjustments,
    rp.blending_factor_sub_region_peril_id,
    rp.applies_to_mga,
    rp.applies_to_prop,
    rp.applies_to_fa,
    bf.air_blend,
    bf.rms_blend,
    bf.base_vendor
from {{ ref('stg_cat_modelling_results__ylts') }} ylt
inner join {{ ref('stg_cat_modelling_results__analysis_lists') }} al 
    on al.analysis_id = ylt.analysis_id 
    and al.source_vendor = ylt.source_vendor
left join {{ ref('stg_reference__hisco_org__lobs') }} lobs 
    on lobs.modelled_lob = al.modelled_lob
left join {{ ref('stg_reference__vor_region_perils') }} rp
    on rp.source_vendor = ylt.source_vendor
    and rp.modelled_region_peril = al.region_peril
left join {{ ref('stg_reference__vor_blending_factors') }} bf
    on bf.sub_region_peril_id = rp.blending_factor_sub_region_peril_id
