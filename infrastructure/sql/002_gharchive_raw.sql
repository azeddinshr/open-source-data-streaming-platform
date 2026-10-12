CREATE TABLE IF NOT EXISTS raw.gharchive_events (
    event_id      TEXT PRIMARY KEY,
    event_type    TEXT        NOT NULL,
    actor_id      BIGINT,
    actor_login   TEXT        NOT NULL,
    repo_id       BIGINT,
    repo_name     TEXT        NOT NULL,
    org_login     TEXT,
    is_public     BOOLEAN,
    created_at    TIMESTAMPTZ NOT NULL,
    action        TEXT,
    archive_date  DATE        NOT NULL,
    archive_hour  SMALLINT    NOT NULL,
    source_file   TEXT        NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_gharchive_created_at
    ON raw.gharchive_events (created_at);