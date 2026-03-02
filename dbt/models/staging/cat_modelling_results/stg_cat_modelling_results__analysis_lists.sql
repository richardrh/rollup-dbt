{{ config(materialized='view') }}

with source as (
    select * from {{ ref('hive_storage__analysis_lists') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'vendor',
            'date',
            'analysis_id',
        ]) }} as pk,

        vendor      as source_vendor,
        date        as run_date,
        analysis_id,
        modelled_lob,
        modelled_peril,
        is_official

    from source
)

select * from renamed
