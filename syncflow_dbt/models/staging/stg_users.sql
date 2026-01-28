with source as (
    select * from {{ source('syncflow_raw', 'raw_users') }}
),

renamed as (
    select
        user_id,
        split_part(email, '@', 2) as email_username,
        company_size,
        signup_at as signed_up_at,
    from source
)
select * from renamed