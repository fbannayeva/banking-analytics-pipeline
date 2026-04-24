-- staging/stg_app_events.sql

with source as (
    select * from {{ source('raw', 'app_events') }}
),

renamed as (
    select
        event_id,
        user_id,
        cast(created_at as timestamp)   as created_at,
        cast(created_at as date)        as event_date,
        lower(event_type)               as event_type,
        lower(platform)                 as platform
    from source
    where event_id is not null
)

select * from renamed
