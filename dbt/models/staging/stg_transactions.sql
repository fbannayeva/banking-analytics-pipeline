-- staging/stg_transactions.sql

with source as (
    select * from {{ source('raw', 'transactions') }}
),

renamed as (
    select
        transaction_id,
        user_id,
        cast(created_at as timestamp)       as created_at,
        cast(created_at as date)            as transaction_date,
        date_trunc('month', cast(created_at as date)) as transaction_month,
        cast(amount as decimal(18,2))       as amount,
        upper(currency)                     as currency,
        lower(type)                         as transaction_type,
        lower(merchant_category)            as merchant_category,
        lower(status)                       as status,
        -- flag completed transactions only
        case when lower(status) = 'completed' then 1 else 0 end as is_completed
    from source
    where transaction_id is not null
      and amount > 0
)

select * from renamed
