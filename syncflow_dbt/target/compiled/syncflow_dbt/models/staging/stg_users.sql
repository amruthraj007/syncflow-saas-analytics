with source as (
    select * from "syncflow"."main"."raw_users"
),

renamed as (
    select
        user_id,
        split_part(email, '@', 2) as email_domain,
        company_size,
        signup_at as signed_up_at,
    from source
)
select * from renamed