{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('forecast_factors') }}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'lob_id',
            'forecast_date',
            'office',
            'class'
        ]) }} as forecast_factor_id,
        lob_id,
        forecast_date,
        factor,
        office,
        class
    from source
)

select *
from renamed
