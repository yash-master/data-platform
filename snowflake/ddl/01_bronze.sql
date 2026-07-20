-- Run against a real Snowflake account when available. Mirrors the same
-- medallion structure as the local Delta Lake pipeline in src/, using
-- Snowflake-native features (streams/tasks) instead of Airflow + watermark
-- files for incremental processing.

create database if not exists streaming_platform;

create schema if not exists streaming_platform.bronze;

create table if not exists streaming_platform.bronze.events (
    user_id            varchar,
    content_id         varchar,
    content_title      varchar,
    genre              varchar,
    event_type         varchar,
    watch_seconds      number,
    device             varchar,
    event_ts           timestamp_ntz,
    ingestion_batch_ts  timestamp_ntz,
    ingestion_run_id   varchar,
    bronze_ingested_at timestamp_ntz default current_timestamp()
)
cluster by (ingestion_batch_ts);

-- External stage pointing at wherever the raw JSON batches land
-- (e.g. an S3/Azure Blob landing zone the streaming app writes to).
create stage if not exists streaming_platform.bronze.raw_events_stage
    file_format = (type = json);

-- Example COPY INTO for a batch load (would be wrapped in a Snowflake
-- Task on a schedule, or triggered by an event notification):
--
-- copy into streaming_platform.bronze.events
-- from @streaming_platform.bronze.raw_events_stage
-- file_format = (type = json)
-- match_by_column_name = case_insensitive;

-- A Stream on Bronze lets the Silver layer process only new rows
-- since the last consumption, replacing the local watermark-file pattern.
create stream if not exists streaming_platform.bronze.events_stream
    on table streaming_platform.bronze.events
    append_only = true;
