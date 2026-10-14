INSERT INTO repo_event_counts_1min
SELECT
    TUMBLE_START(created_at, INTERVAL '1' MINUTE)  AS window_start,
    TUMBLE_END(created_at, INTERVAL '1' MINUTE)    AS window_end,
    repo_name,
    event_type,
    COUNT(*)                                        AS event_count,
    COUNT(DISTINCT actor_login)                     AS unique_actors
FROM github_events
GROUP BY
    TUMBLE(created_at, INTERVAL '1' MINUTE),
    repo_name,
    event_type;
