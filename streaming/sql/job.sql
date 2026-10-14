CREATE TABLE github_events (
    event_id          STRING,
    event_type        STRING,
    repo_name         STRING,
    actor_login       STRING,
    created_at_raw    STRING,
    ingested_at       STRING,
    payload           STRING,
    created_at        AS TO_TIMESTAMP(
                          SUBSTR(created_at_raw, 1, 19),
                          'yyyy-MM-dd''T''HH:mm:ss'
                      ),
    WATERMARK FOR created_at AS created_at - INTERVAL '10' SECOND
) WITH (
    'connector'                       = 'kafka',
    'topic'                           = 'github-events',
    'properties.bootstrap.servers'    = 'redpanda:9092',
    'properties.group.id'             = 'flink-repo-counts-v2',
    'scan.startup.mode'               = 'latest-offset',
    'format'                          = 'json',
    'json.ignore-parse-errors'        = 'true'
)
;

CREATE TABLE repo_event_counts_1min (
    window_start   TIMESTAMP(3),
    window_end     TIMESTAMP(3),
    repo_name      STRING,
    event_type     STRING,
    event_count    BIGINT,
    unique_actors  BIGINT,
    PRIMARY KEY (window_start, repo_name, event_type) NOT ENFORCED
) WITH (
    'connector'                   = 'jdbc',
    'url'                         = 'jdbc:postgresql://postgres:5432/analytics',
    'table-name'                  = 'streaming.repo_event_counts_1min',
    'username'                    = 'de_user',
    'password'                    = 'de_password',
    'sink.buffer-flush.max-rows'  = '100',
    'sink.buffer-flush.interval'  = '5s'
)
;

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
    event_type
;

