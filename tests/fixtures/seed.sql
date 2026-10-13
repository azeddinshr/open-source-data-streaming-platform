INSERT INTO raw.github_events
    (event_id, event_type, repo_name, actor_login, created_at, ingested_at, payload)
VALUES
    ('1001', 'PushEvent', 'acme/app', 'alice', '2026-10-12 11:05:00+00', now(),
     '{"actor":{"id":1},"repo":{"id":10},"public":true}'::jsonb),
    ('1002', 'PullRequestEvent', 'acme/app', 'dependabot[bot]', '2026-10-12 11:06:00+00', now(),
     '{"actor":{"id":2},"repo":{"id":10},"public":true,"payload":{"action":"opened"}}'::jsonb);

INSERT INTO raw.gharchive_events
    (event_id, event_type, actor_id, actor_login, repo_id, repo_name,
     is_public, created_at, action, archive_date, archive_hour, source_file)
VALUES
    ('1001', 'PushEvent', 1, 'alice', 10, 'acme/app',
     true, '2026-10-12 11:05:00+00', NULL, '2026-10-12', 11, '2026-10-12-11.json.gz'),
    ('1003', 'CreateEvent', 3, 'carol', 11, 'acme/lib',
     true, '2026-10-12 11:07:00+00', NULL, '2026-10-12', 11, '2026-10-12-11.json.gz');