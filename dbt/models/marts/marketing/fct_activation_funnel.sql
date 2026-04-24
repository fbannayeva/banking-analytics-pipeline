-- marts/marketing/fct_activation_funnel.sql
-- Conversion rates at each step of the onboarding funnel

with funnel as (
    select * from {{ ref('int_onboarding_funnel') }}
),

monthly_funnel as (
    select
        signup_month,
        country,
        plan_type,

        count(distinct user_id)                                         as total_registered,

        count(distinct case when step_kyc_verified = 1
              then user_id end)                                         as total_kyc_verified,

        count(distinct case when step_card_activated = 1
              then user_id end)                                         as total_card_activated,

        count(distinct case when step_first_transfer = 1
              then user_id end)                                         as total_fully_activated,

        -- conversion rates
        round(100.0 * count(distinct case when step_kyc_verified = 1
              then user_id end)
              / nullif(count(distinct user_id), 0), 2)                 as kyc_conversion_rate,

        round(100.0 * count(distinct case when step_card_activated = 1
              then user_id end)
              / nullif(count(distinct user_id), 0), 2)                 as card_activation_rate,

        round(100.0 * count(distinct case when step_first_transfer = 1
              then user_id end)
              / nullif(count(distinct user_id), 0), 2)                 as full_activation_rate,

        -- time to activate (median)
        percentile_cont(0.5) within group
              (order by days_to_first_transfer)                        as median_days_to_activate

    from funnel
    group by 1, 2, 3
)

select * from monthly_funnel
