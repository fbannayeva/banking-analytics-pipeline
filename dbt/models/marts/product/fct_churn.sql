-- marts/product/fct_churn.sql
-- Monthly churn: user is churned if inactive for 30+ days

with users as (
    select user_id, signup_month, country, plan_type
    from {{ ref('stg_users') }}
    where kyc_status = 'verified'
),

activity as (
    select user_id, activity_date
    from {{ ref('int_user_activity') }}
),

last_activity as (
    select
        user_id,
        max(activity_date) as last_active_date
    from activity
    group by 1
),

churn_flags as (
    select
        u.user_id,
        u.signup_month,
        u.country,
        u.plan_type,
        la.last_active_date,
        datediff('day', la.last_active_date, current_date) as days_inactive,
        case
            when la.last_active_date is null then 1               -- never active
            when datediff('day', la.last_active_date, current_date) >= 30 then 1
            else 0
        end as is_churned
    from users u
    left join last_activity la on u.user_id = la.user_id
),

monthly_churn as (
    select
        signup_month,
        country,
        plan_type,
        count(distinct user_id)                         as total_users,
        count(distinct case when is_churned = 1
              then user_id end)                         as churned_users,
        round(100.0 * count(distinct case when is_churned = 1
              then user_id end) / nullif(count(distinct user_id), 0), 2) as churn_rate
    from churn_flags
    group by 1, 2, 3
)

select * from monthly_churn
