{{ config(materialized='view') }}

with source as (
    select * from {{ ref('hive_storage__raw_analysis_lists') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key([
        'source',
        'analysis_id',
        'filename',
        'run_date',
        ]) }} as pk,

        source as source_vendor,
        date as run_date,
        filename as source_file,
        analysis_id,
        modelled_lob,
        region_peril,
        is_official,
        AAL,
        OEP_200,
        OEP_1000,
        AEP_200,
        AEP_1000
    from source
)

select * from renamed
