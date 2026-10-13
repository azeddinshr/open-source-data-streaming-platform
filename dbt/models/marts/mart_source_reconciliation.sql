with by_hour_type as (

    select
        date_trunc('hour', created_at)                         as event_hour,
        event_type,
        count(*) filter (where in_archive)                     as archive_events,
        count(*) filter (where in_stream)                      as stream_events,
        count(*) filter (where in_archive and in_stream)       as in_both,
        count(*) filter (where in_stream and not in_archive)   as stream_only
    from {{ ref('fct_events_unified') }}
    group by 1, 2

),

hours_with_archive as (
    select event_hour
    from by_hour_type
    group by 1
    having sum(archive_events) > 0
)

select
    b.*,
    round(100.0 * b.in_both / nullif(b.archive_events, 0), 2)   as stream_capture_pct,
    round(100.0 * b.stream_only / nullif(b.stream_events, 0), 1) as stream_only_pct
from by_hour_type b
join hours_with_archive h using (event_hour)