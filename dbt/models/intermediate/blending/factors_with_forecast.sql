{{ config(materialized='view') }}

select
    bfa.*,
    ff.office as forecast_factor_office,
    ff."class" as forecast_factor_class,
    coalesce(ff.f_202601, 1.0) as f_202601,
    coalesce(ff.f_202607, 1.0) as f_202607,
    coalesce(ff.f_202701, 1.0) as f_202701
from {{ ref('factors_applied') }} as bfa
left join {{ ref('forecast_factors_with_lobs_to_apply') }} as ff
    on ff.lob_id = bfa.lob_id
