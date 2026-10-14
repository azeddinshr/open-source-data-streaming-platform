CREATE TABLE github_events (
    `type`       STRING,
    `actor`      ROW<`login` STRING>,
    `repo`       ROW<`name` STRING>,
    `created_at` STRING,
    event_ts AS TO_TIMESTAMP(
                    SUBSTR(`created_at`, 1, 19),
                    'yyyy-MM-dd''T''HH:mm:ss'
                ),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
) WITH (
    'connector'                       = 'kafka',
    'topic'                           = 'github-events',
    'properties.bootstrap.servers'    = 'redpanda:9092',
    'properties.group.id'             = 'flink-repo-counts-v3',
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
    TUMBLE_START(event_ts, INTERVAL '1' MINUTE)  AS window_start,
    TUMBLE_END(event_ts, INTERVAL '1' MINUTE)    AS window_end,
    `repo`.`name`                                AS repo_name,
    `type`                                       AS event_type,
    COUNT(*)                                     AS event_count,
    COUNT(DISTINCT `actor`.`login`)              AS unique_actors
FROM github_events
GROUP BY
    TUMBLE(event_ts, INTERVAL '1' MINUTE),
    `repo`.`name`,
    `type`
;
