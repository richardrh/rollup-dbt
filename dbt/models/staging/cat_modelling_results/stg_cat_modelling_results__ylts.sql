{{ config(materialized='view') }}

with source as (
    select * from {{ ref('hive_storage__raw_ylts') }}
),

keyed as (
    select
        {{ dbt_utils.generate_surrogate_key([
        'vendor',
        'date',
        'model_code',
        'year_id',
        'event_id',
        'analysis_id'
        ]) }} as pk,

        sha256(concat_ws('|', analysis_id, model_code, vendor, filename, date))
        as aggregation_key,

        vendor as source_vendor,
        date as run_date,
        filename as source_file,
        analysis_id,
        model_code,
        year_id,
        event_id,
        loss

    from source
)

select * from keyed
