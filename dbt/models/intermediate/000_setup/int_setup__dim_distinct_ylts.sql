{{ config(materialized='table') }}

/*
    Dimension lookup for the RiskLink EP aggregation key.

    Provides one row per unique aggregation_key, mapping the opaque hash back
    to the human-readable dimension columns that were used to produce it.
    Downstream mart models join this on aggregation_key to recover labels.
*/

select distinct
    ylt.aggregation_key,
    ylt.run_date,
    ylt.source_vendor,
    ylt.source_file,
    ylt.analysis_id,
    al.modelled_lob,
    lobs.office,
    lobs.class,
    date_trunc('month', ylt.run_date) as base_date
from {{ ref('stg_cat_modelling_results__ylts') }} ylt
inner join {{ ref('stg_cat_modelling_results__analysis_lists') }} al 
    on al.analysis_id = ylt.analysis_id 
    and al.source_vendor = ylt.source_vendor
left join {{ ref('stg_reference__hisco_org__lobs') }} lobs 
    on lobs.modelled_lob = al.modelled_lob
