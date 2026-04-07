{{ config(materialized='table') }}

/*
    Final rollup mart: forecasted blended EP curves.
    Provides one row per (office, class, modelled_lob, peril, ep_type, return_period, metric_name).
    This is the consumer-facing output of the catastrophe model blending pipeline.
*/

with forecast as (
    select *
    from {{ ref('int_forecast') }}
)

select
    office,
    class,
    modelled_lob,
    modelled_peril as peril,
    base_date,
    forecast_date,
    ep_type,
    return_period,
    metric_name,
    original_value,
    forecast_factor,
    forecasted_value,
    air_blend,
    rms_blend
from forecast
