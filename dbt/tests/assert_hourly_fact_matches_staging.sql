select
    (select count(*) from {{ ref('stg_github_events') }})   as staging_rows,
    (select sum(event_count) from {{ ref('fct_events_hourly') }}) as fact_rows
where
    (select count(*) from {{ ref('stg_github_events') }})
    <> (select sum(event_count) from {{ ref('fct_events_hourly') }})