{{ config(materialized='view') }}

with source as (
    select * from {{ ref('hive_storage__raw_ylts') }}
),

, surrogate_keyed_and_extracted_filename as (
    select

        -- Hash and file info
        {{ dbt_utils.generate_surrogate_key([
            'vendor',
            'date',
            'analysis_id',
            'model_id',
            'year_id',
            'event_id',
        ]) }} as pk,

        regexp_extract(filename, '[^/]+\.parquet$')            as source_file,
        *,

    from source
)

, renamed as (
    select
        pk,
        sha256(concat_ws('||',
            vendor,
            date,
            source_file,
            analysis_id
        ))
        as aggregation_key,

        source_file,
        vendor                                                 as source_vendor,
        date                                                   as run_date,
        analysis_id,

        -- Modelling info
        model_id,
        year_id,
        event_id,
        loss,

        -- De-dupe check
        row_number() over (
            partition by pk
            order by loss
        ) as rn

    from source
)

-- Dedup safety net: with a correctly defined pk, any rn > 1 rows are exact
-- duplicates (same event, same model version, same loss). order by loss is a
-- harmless tie-breaker — all candidates share the same loss value.
select * exclude(rn) from renamed
where rn = 1
