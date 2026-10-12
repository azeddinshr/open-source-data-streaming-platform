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
    count(distinct repo_id)                 as unique_repos
from {{ ref('stg_github_events') }}

{% if is_incremental() %}
-- reprocess the last 3 hours to absorb late-arriving events
where date_trunc('hour', created_at) >= (
    select coalesce(max(event_hour), '1970-01-01'::timestamptz) - interval '3 hours'
    from {{ this }}
)
{% endif %}

group by 1, 2, 3, 4