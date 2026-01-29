
  
    
    

    create  table
      "syncflow"."main"."int_user_activities__dbt_tmp"
  
    as (
      with events as (
    -- Aggregate first to keep the data volume low
    select 
        user_id,
        count(case when event_type = 'project_created' then 1 end) as total_project_creations,
        count(case when event_type = 'task_added' then 1 end) as total_tasks_added,
        count(case when event_type = 'teammate_invited' then 1 end) as total_teammate_invites,
        count(case when event_type = 'integration_connected' then 1 end) as total_integration_connected,
        min(occurred_at) as first_activity_at,
        max(occurred_at) as last_activity_at
    from "syncflow"."main"."stg_events"
    group by 1
),

final as (
    -- Left join ensures we keep users with 0 activity
    select
        u.user_id,
        coalesce(e.total_project_creations, 0) as total_project_creations,
        coalesce(e.total_tasks_added, 0) as total_tasks_added,
        coalesce(e.total_teammate_invites, 0) as total_teammate_invites,
        coalesce(e.total_integration_connected, 0) as total_integration_connected,
        e.first_activity_at,
        e.last_activity_at
    from "syncflow"."main"."stg_users" u
    left join events e on u.user_id = e.user_id
)

select * from final
    );
  
  