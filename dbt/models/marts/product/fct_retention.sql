-- marts/product/fct_retention.sql
-- Cohort retention: D1, D7, D30 per signup month

with users as (
    select user_id, signup_date, signup_month, country, plan_type
    from {{ ref('stg_users') }}
    where kyc_status = 'verified'
),

activity as (
    select user_id, activity_date
    from {{ ref('int_user_activity') }}
),

cohort_activity as (
    select
        u.user_id,
        u.signup_date,
        u.signup_month,
        u.country,
        u.plan_type,
        a.activity_date,
        datediff('day', u.signup_date, a.activity_date) as day_number
    from users u
    left join activity a on u.user_id = a.user_id
),

retention as (
    select
        signup_month,
        country,
        plan_type,
        count(distinct user_id)                                         as cohort_size,

        -- D1 retention
        count(distinct case when day_number = 1  then user_id end)      as retained_d1,
        -- D7 retention
        count(distinct case when day_number = 7  then user_id end)      as retained_d7,
        -- D30 retention
        count(distinct case when day_number = 30 then user_id end)      as retained_d30

    from cohort_activity
    group by 1, 2, 3
),

final as (
    select
        signup_month,
        country,
        plan_type,
        cohort_size,
        retained_d1,
        retained_d7,
        retained_d30,
        round(100.0 * retained_d1  / nullif(cohort_size, 0), 2)  as retention_rate_d1,
        round(100.0 * retained_d7  / nullif(cohort_size, 0), 2)  as retention_rate_d7,
        round(100.0 * retained_d30 / nullif(cohort_size, 0), 2)  as retention_rate_d30
    from retention
)

select * from final
