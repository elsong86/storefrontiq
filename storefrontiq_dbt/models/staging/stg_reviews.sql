{{ config(materialized='view') }}

with source as (
    select * from {{ source('silver', 'silver_reviews_ext') }}
),

renamed as (
    select
        value:ingestion_id::string       as ingestion_id,
        value:place_id::string            as place_id,
        value:business_name::string       as business_name,
        value:address::string             as address,
        value:source::string              as source,
        value:ingested_at::timestamp_ntz  as ingested_at,
        week_start                        as week_start,
        value:raw_review_text::string     as raw_review_text,
        value:cleaned_text::string        as cleaned_text,
        value:sentiment_score::float      as sentiment_score,
        value:sentiment_label::string     as sentiment_label,
        value:word_count::int             as word_count,
        value:char_count::int             as char_count,
        value:processed_at::timestamp_ntz as processed_at
    from source
)

select * from renamed
