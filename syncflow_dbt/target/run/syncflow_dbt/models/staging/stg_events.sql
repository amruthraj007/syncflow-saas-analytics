
  
  create view "syncflow"."main"."stg_events__dbt_tmp" as (
    with source as (
    select * from "syncflow"."main"."raw_events"
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
  );
