{{ config(materialized='table') }}

/*
    Dimension lookup for the RiskLink EP aggregation key.

    Provides one row per unique aggregation_key, mapping the opaque hash back
    to the human-readable dimension columns that were used to produce it.
    Downstream mart models join this on aggregation_key to recover labels.
*/

select distinct
    aggregation_key,
    run_date,
    source_file,
    analysis_id
from {{ ref('stg_risklink__ylts') }}
