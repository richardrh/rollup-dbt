{{ config(materialized='view') }}

-- Staging view for Risklink ELT (Event Loss Table)
-- This is the raw data before simulation
-- Note: In production, this references the raw ELT table in staging schema

with source as (
    select *
    from {{ source('cat_models_raw', 'stg_risklink__elts') }}
)


select
    analysis_id,
    eventid as event_id,
    rate,
    perspcode,
    perspvalue as meanloss,
    stddevi as stddevi,
    stddevc as stddevc,
    expvalue as expvalue
from source
