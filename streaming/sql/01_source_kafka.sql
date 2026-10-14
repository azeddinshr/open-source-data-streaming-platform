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
);
