{{ config(materialized='view') }}

-- Staging view for Risklink ELT (Event Loss Table)
-- This is the raw data before simulation
-- Note: In production, this references the raw ELT table in staging schema

select
    eventid as event_id,
    rate,
    meanloss as mean_loss,
    stddev as std_dev,
    expvalue as exp_value
from {{ source('cat_models_raw', 'stg_risklink__elts') }}
