{{
    config(
        materialized='incremental',
        unique_key='hour_key',
        incremental_strategy='delete+insert'
    )
}}

select
    md5(
        date_trunc('hour', created_at)::text || '|' || event_type || '|' || is_bot::text
    )                                       as hour_key,
    date_trunc('hour', created_at)          as event_hour,
    event_type,
    is_bot,
    count(*)                                as event_count,
    count(distinct actor_id)                as unique_actors,
    count(distinct repo_id)                 as unique_repos,
    max(loaded_at)                          as last_loaded_at
from {{ ref('stg_github_events') }}

{% if is_incremental() %}
-- recompute every hour that received newly loaded events, however old its created_at
where date_trunc('hour', created_at) in (
    select distinct date_trunc('hour', created_at)
    from {{ ref('stg_github_events') }}
    where loaded_at > (
        select coalesce(max(last_loaded_at), '1970-01-01'::timestamptz) - interval '10 minutes'
        from {{ this }}
    )
)
{% endif %}

group by 1, 2, 3, 4