{{ config(materialized='view') }}

with valid_analysis as (
    select
        *
from {{ ref('analysis_is_valid') }}
),
ranked_ylt as (
    select
        *
from {{ ref('ylt_combined_ranked_bucketed') }}
)
select
    ylt.*,
    va.official_rollup
from ranked_ylt as ylt
inner join valid_analysis as va
    on va.lob_id = ylt.lob_id
    and va.region_peril_id = ylt.region_peril_id
