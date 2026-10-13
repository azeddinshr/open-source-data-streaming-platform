select
    event_id,
    event_type,
    created_at,
    loaded_at,
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    split_part(repo_name, '/', 1)   as repo_owner,
    split_part(repo_name, '/', 2)   as repo_short_name,
    org_login,
    action,
    coalesce(is_public, true)       as is_public,
    (actor_login like '%[bot]')     as is_bot,
    archive_date,
    archive_hour
from {{ source('raw', 'gharchive_events') }}