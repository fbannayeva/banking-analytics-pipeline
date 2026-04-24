-- staging/stg_cards.sql

with source as (
    select * from {{ source('raw', 'cards') }}
),

renamed as (
    select
        card_id,
        user_id,
        cast(created_at as timestamp)   as created_at,
        cast(created_at as date)        as issued_date,
        lower(card_type)                as card_type,
        lower(status)                   as card_status,
        lower(plan_type)                as plan_type
    from source
    where card_id is not null
)

select * from renamed
