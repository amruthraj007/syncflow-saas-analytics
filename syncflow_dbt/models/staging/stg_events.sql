with source as (
    select * from {{ source('syncflow_raw', 'raw_events') }}
),
renamed as (
    select
        event_id,
        user_id,
        event_type,
        event_time as occurred_at,
    from source
)
select * from renamed