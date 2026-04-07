{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('vor_euws_rate_factors') }}
),

renamed as (
    select

        {{ dbt_utils.generate_surrogate_key([
            'ModelEventID',
            'OccYear'
        ]) }} as euws_rate_factor_id,

        ModelEventID as model_event_id,
        OccYear      as occ_year,
        Factor       as factor

    from source
)

select * from renamed
