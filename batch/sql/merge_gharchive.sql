INSERT INTO raw.gharchive_events (
    event_id, event_type, actor_id, actor_login, repo_id, repo_name,
    org_login, is_public, created_at, action, archive_date, archive_hour, source_file
)
SELECT
    event_id, event_type, actor_id, actor_login, repo_id, repo_name,
    org_login, is_public, created_at AT TIME ZONE 'UTC', action,
    archive_date, archive_hour, source_file
FROM raw.gharchive_events_staging
ON CONFLICT (event_id) DO NOTHING;