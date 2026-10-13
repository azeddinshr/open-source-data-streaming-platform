select 1
where (select count(*) from {{ ref('fct_events_unified') }})
   <> (select count(*) from {{ ref('stg_gharchive_events') }})
    + (select count(*)
       from {{ ref('stg_github_events') }} s
       where not exists (
           select 1 from {{ ref('stg_gharchive_events') }} a
           where a.event_id = s.event_id
       ))