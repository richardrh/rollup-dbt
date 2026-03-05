
{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('vor_region_perils') }}
)

, renamed as (
    select

        {{ dbt_utils.generate_surrogate_key([
            'id',
            'vendor',
            'modelled_region_peril',
            'cleaned_region_peril',

        ]) }} as region_peril_id,
    id,
    vendor,
    modelled_region_peril,
    cleaned_region_peril,
    region,
    peril,
    adjustments,
    applies_to_mga,
    appplies_to_prop,
    applies_to_fa

    from source
)

select * from renamed
