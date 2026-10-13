select
    repo_id,
    max(repo_name)                                             as repo_name,
    max(repo_owner)                                            as repo_owner,
    count(*)                                                   as total_events,
    count(*) filter (where event_type = 'PushEvent')           as push_events,
    count(*) filter (where event_type = 'PullRequestEvent')    as pull_request_events,
    count(*) filter (where event_type = 'IssuesEvent')         as issue_events,
    count(*) filter (where event_type = 'WatchEvent')          as star_events,
    count(distinct actor_id)                                   as unique_actors,
    round(100.0 * count(*) filter (where is_bot) / count(*), 1) as bot_event_pct,
    min(created_at)                                            as first_seen_at,
    max(created_at)                                            as last_seen_at
from {{ ref('stg_github_events') }}
group by 1