{{ config(materialized='view') }}

with source as (
    select * from {{ ref('risklink__ylts') }}
),

renamed as (
    select

        -- Generate string pk
        {{ dbt_utils.generate_surrogate_key([
            'date',
            'filename',
            'analysis_id',
            'year_id',
            'event_id',
        ]) }} as pk,


        -- Do same but without year and event to use as aggregation key
        sha256(concat('||',
            date, '||',
            regexp_extract(filename, '[^/]+\.parquet$'), '||',
            analysis_id, '||'))
        as aggregation_key,

        -- hive partition metadata
        date                                                as run_date,
        regexp_extract(filename, '[^/]+\.parquet$')         as source_file,

        -- data columns
        analysis_id,
        year_id,
        event_id,
        loss

    from source
)

select * from renamed
