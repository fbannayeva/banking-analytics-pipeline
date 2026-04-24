-- marts/product/dim_users.sql
-- Enriched user dimension: one row per user with lifetime stats

with users as (
    select * from {{ ref('stg_users') }}
),

transactions as (
    select * from {{ ref('stg_transactions') }}
    where status = 'completed'
),

cards as (
    select * from {{ ref('stg_cards') }}
),

user_tx_stats as (
    select
        user_id,
        count(*)                        as total_transactions,
        sum(amount)                     as lifetime_value,
        avg(amount)                     as avg_transaction_amount,
        min(transaction_date)           as first_transaction_date,
        max(transaction_date)           as last_transaction_date,
        count(distinct transaction_month) as active_months
    from transactions
    group by 1
),

user_cards as (
    select
        user_id,
        count(*)                        as total_cards,
        max(case when card_type = 'physical' then 1 else 0 end) as has_physical_card
    from cards
    group by 1
),

final as (
    select
        u.user_id,
        u.created_at,
        u.signup_date,
        u.signup_month,
        u.country,
        u.device_type,
        u.kyc_status,
        u.plan_type,
        u.age_bucket,

        -- transaction stats
        coalesce(t.total_transactions, 0)       as total_transactions,
        coalesce(t.lifetime_value, 0)           as lifetime_value,
        coalesce(t.avg_transaction_amount, 0)   as avg_transaction_amount,
        t.first_transaction_date,
        t.last_transaction_date,
        coalesce(t.active_months, 0)            as active_months,

        -- card stats
        coalesce(c.total_cards, 0)              as total_cards,
        coalesce(c.has_physical_card, 0)        as has_physical_card,

        -- derived segments
        case
            when coalesce(t.total_transactions, 0) = 0    then 'dormant'
            when coalesce(t.total_transactions, 0) < 5    then 'low'
            when coalesce(t.total_transactions, 0) < 20   then 'medium'
            else 'high'
        end                                             as engagement_segment,

        -- days since last activity
        datediff('day', t.last_transaction_date, current_date) as days_since_last_tx

    from users u
    left join user_tx_stats t on u.user_id = t.user_id
    left join user_cards c on u.user_id = c.user_id
)

select * from final
