create schema if not exists streaming_platform.silver;

create table if not exists streaming_platform.silver.events (
    user_id             varchar,
    content_id          varchar,
    content_title       varchar,
    genre               varchar,
    event_type          varchar,
    watch_seconds       number,
    device              varchar,
    event_ts            timestamp_ntz,
    ingestion_batch_ts   timestamp_ntz,
    ingestion_run_id    varchar,
    bronze_ingested_at  timestamp_ntz,
    silver_processed_at timestamp_ntz default current_timestamp()
)
cluster by (ingestion_batch_ts);

-- Task: consumes only new rows from the Bronze stream (equivalent to the
-- local pipeline's watermark file), dedupes, and fills missing device with
-- an explicit sentinel rather than leaving it NULL.
create task if not exists streaming_platform.silver.build_silver_events
    warehouse = transform_wh
    schedule = 'USING CRON 0 * * * * UTC'
    when system$stream_has_data('streaming_platform.bronze.events_stream')
as
insert into streaming_platform.silver.events (
    user_id, content_id, content_title, genre, event_type,
    watch_seconds, device, event_ts, ingestion_batch_ts,
    ingestion_run_id, bronze_ingested_at
)
select
    user_id,
    content_id,
    content_title,
    genre,
    event_type,
    watch_seconds,
    coalesce(device, 'unknown') as device,
    event_ts,
    ingestion_batch_ts,
    ingestion_run_id,
    bronze_ingested_at
from (
    select
        *,
        row_number() over (
            partition by user_id, content_id, event_type, event_ts
            order by bronze_ingested_at desc
        ) as rn
    from streaming_platform.bronze.events_stream
)
where rn = 1;
