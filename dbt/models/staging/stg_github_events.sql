with source as (

    select * from {{ source('raw', 'github_events') }}

),

parsed as (

    select
        event_id,
        event_type,
        created_at,
        ingested_at,
        loaded_at,

        -- actor
        (payload -> 'actor' ->> 'id')::bigint       as actor_id,
        actor_login,

        -- repo
        (payload -> 'repo' ->> 'id')::bigint        as repo_id,
        repo_name,
        split_part(repo_name, '/', 1)               as repo_owner,
        split_part(repo_name, '/', 2)               as repo_short_name,

        -- org (null for personal repos)
        payload -> 'org' ->> 'login'                as org_login,

        -- event details
        payload -> 'payload' ->> 'action'           as action,
        coalesce((payload ->> 'public')::boolean, true) as is_public,

        -- bots are a big share of traffic; flag them for analytics
        (actor_login like '%[bot]')                 as is_bot

    from source

)

select * from parsed