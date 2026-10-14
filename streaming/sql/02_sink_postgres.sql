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
);
