{{ config(materialized='table') }}

with ep_curves as (
    {{ ep_curve_from_ylt(
        ylt_ref=ref('stg_risklink__ylts'),
        loss_column='loss',
        n_simulations=10000,
        key_column='aggregation_key'
    ) }}
),

dim_lookup as (
    select * from {{ ref('int_ep__risklink_dim_lookup') }}
)

select
    d.aggregation_key,
    d.run_date,
    d.source_file,
    d.analysis_id,
    ep.ep_type,
    ep.return_period,
    ep.rank_num,
    ep.annual_loss
from ep_curves ep
join dim_lookup d using (aggregation_key)
