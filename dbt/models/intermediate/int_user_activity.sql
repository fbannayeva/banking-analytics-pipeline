-- intermediate/int_user_activity.sql
-- Daily activity summary per user: used by retention and churn models

with events as (
    select * from {{ ref('stg_app_events') }}
),

transactions as (
    select * from {{ ref('stg_transactions') }}
    where status = 'completed'
),

daily_events as (
    select
        user_id,
        event_date                      as activity_date,
        count(*)                        as n_events,
        count(distinct event_type)      as unique_event_types
    from events
    group by 1, 2
),

daily_transactions as (
    select
        user_id,
        transaction_date                as activity_date,
        count(*)                        as n_transactions,
        sum(amount)                     as total_amount,
        avg(amount)                     as avg_amount
    from transactions
    group by 1, 2
),

combined as (
    select
        coalesce(e.user_id, t.user_id)          as user_id,
        coalesce(e.activity_date, t.activity_date) as activity_date,
        coalesce(e.n_events, 0)                 as n_events,
        coalesce(e.unique_event_types, 0)        as unique_event_types,
        coalesce(t.n_transactions, 0)            as n_transactions,
        coalesce(t.total_amount, 0)              as total_amount,
        coalesce(t.avg_amount, 0)                as avg_transaction_amount,
        -- active if has event OR transaction
        1                                        as is_active
    from daily_events e
    full outer join daily_transactions t
        on e.user_id = t.user_id
        and e.activity_date = t.activity_date
)

select * from combined
