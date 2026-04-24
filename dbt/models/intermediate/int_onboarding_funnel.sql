-- intermediate/int_onboarding_funnel.sql
-- Tracks each user through the activation funnel:
-- registered → kyc_verified → card_activated → first_transfer

with users as (
    select * from {{ ref('stg_users') }}
),

events as (
    select * from {{ ref('stg_app_events') }}
),

card_activated as (
    select user_id, min(event_date) as card_activated_date
    from events
    where event_type = 'card_activated'
    group by 1
),

first_transfer as (
    select user_id, min(event_date) as first_transfer_date
    from events
    where event_type = 'first_transfer'
    group by 1
),

funnel as (
    select
        u.user_id,
        u.signup_date,
        u.signup_month,
        u.country,
        u.plan_type,
        u.kyc_status,

        -- step 1: registered (all users)
        1                                               as step_registered,

        -- step 2: kyc verified
        case when u.kyc_status = 'verified' then 1
             else 0 end                                 as step_kyc_verified,

        -- step 3: card activated
        case when ca.card_activated_date is not null
             then 1 else 0 end                          as step_card_activated,

        -- step 4: first transfer (fully activated)
        case when ft.first_transfer_date is not null
             then 1 else 0 end                          as step_first_transfer,

        ca.card_activated_date,
        ft.first_transfer_date,

        -- days to activate from signup
        datediff('day', u.signup_date, ca.card_activated_date) as days_to_card_activation,
        datediff('day', u.signup_date, ft.first_transfer_date) as days_to_first_transfer
    from users u
    left join card_activated ca on u.user_id = ca.user_id
    left join first_transfer ft on u.user_id = ft.user_id
)

select * from funnel
