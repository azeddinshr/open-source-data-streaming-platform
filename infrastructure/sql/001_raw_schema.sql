CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.github_events (
    event_id         TEXT PRIMARY KEY,
    event_type       TEXT        NOT NULL,
    repo_name        TEXT        NOT NULL,
    actor_login      TEXT        NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL,
    ingested_at      TIMESTAMPTZ,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    kafka_partition  INT,
    kafka_offset     BIGINT,
    payload          JSONB       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_github_events_created_at
    ON raw.github_events (created_at);
CREATE INDEX IF NOT EXISTS idx_github_events_type
    ON raw.github_events (event_type);
