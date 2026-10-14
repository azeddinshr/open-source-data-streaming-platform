CREATE SCHEMA IF NOT EXISTS streaming;

CREATE TABLE IF NOT EXISTS streaming.repo_event_counts_1min (
    window_start   TIMESTAMP(3) NOT NULL,
    window_end     TIMESTAMP(3) NOT NULL,
    repo_name      TEXT         NOT NULL,
    event_type     TEXT         NOT NULL,
    event_count    BIGINT       NOT NULL,
    unique_actors  BIGINT       NOT NULL,
    PRIMARY KEY (window_start, repo_name, event_type)
);

CREATE INDEX IF NOT EXISTS idx_repo_event_counts_window
    ON streaming.repo_event_counts_1min (window_start DESC);
