-- staging/stg_users.sql
-- Rename, cast, and light cleaning only. No business logic here.

with source as (
    select * from {{ source('raw', 'users') }}
),

renamed as (
    select
        user_id,
        cast(created_at as timestamp)   as created_at,
        lower(country)                  as country,
        lower(device_type)              as device_type,
        lower(kyc_status)               as kyc_status,
        lower(plan_type)                as plan_type,
        lower(age_bucket)               as age_bucket,
        -- derived
        cast(created_at as date)        as signup_date,
        date_trunc('month', cast(created_at as date)) as signup_month
    from source
    where user_id is not null
)

select * from renamed
