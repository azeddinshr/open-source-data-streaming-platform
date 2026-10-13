{{ config(materialized='table') }}

with stream as (
    select event_id, event_type, created_at, actor_id, actor_login, repo_id,
           repo_name, repo_owner, org_login, is_public, is_bot
    from {{ ref('stg_github_events') }}
),

archive as (
    select event_id, event_type, created_at, actor_id, actor_login, repo_id,
           repo_name, repo_owner, org_login, is_public, is_bot
    from {{ ref('stg_gharchive_events') }}
)

select
    coalesce(a.event_id, s.event_id)        as event_id,
    coalesce(a.event_type, s.event_type)    as event_type,
    coalesce(a.created_at, s.created_at)    as created_at,
    coalesce(a.actor_id, s.actor_id)        as actor_id,
    coalesce(a.actor_login, s.actor_login)  as actor_login,
    coalesce(a.repo_id, s.repo_id)          as repo_id,
    coalesce(a.repo_name, s.repo_name)      as repo_name,
    coalesce(a.repo_owner, s.repo_owner)    as repo_owner,
    coalesce(a.org_login, s.org_login)      as org_login,
    coalesce(a.is_public, s.is_public)      as is_public,
    coalesce(a.is_bot, s.is_bot)            as is_bot,
    (a.event_id is not null)                as in_archive,
    (s.event_id is not null)                as in_stream
from archive a
full outer join stream s on a.event_id = s.event_id