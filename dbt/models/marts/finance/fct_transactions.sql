-- marts/finance/fct_transactions.sql
-- Clean transaction facts for BI consumption

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

users as (
    select user_id, country, plan_type, age_bucket
    from {{ ref('stg_users') }}
),

final as (
    select
        t.transaction_id,
        t.user_id,
        t.created_at,
        t.transaction_date,
        t.transaction_month,
        t.amount,
        t.currency,
        t.transaction_type,
        t.merchant_category,
        t.status,
        t.is_completed,
        -- enrich with user dims
        u.country,
        u.plan_type,
        u.age_bucket
    from transactions t
    left join users u on t.user_id = u.user_id
)

select * from final
