create schema if not exists streaming_platform.gold;

-- Gold: daily user-level features, the feature-store surface for
-- model training / BI. Modeled as a view over Silver so it's always
-- current without a separate materialization job — swap to a Dynamic
-- Table (`create dynamic table ... target_lag = '1 hour'`) if the
-- underlying Silver table is large enough that recomputing on read
-- becomes expensive.

create or replace view streaming_platform.gold.user_features as
with daily_watch as (
    select
        user_id,
        date_trunc('day', event_ts) as event_date,
        sum(watch_seconds) as total_watch_seconds,
        count(distinct content_id) as distinct_titles_watched,
        sum(case when event_type = 'play_complete' then 1 else 0 end) as completions,
        sum(case when event_type = 'abandon' then 1 else 0 end) as abandons
    from streaming_platform.silver.events
    group by 1, 2
),

genre_time as (
    select
        user_id,
        genre,
        sum(watch_seconds) as genre_watch_seconds
    from streaming_platform.silver.events
    group by 1, 2
),

top_genre as (
    select
        user_id,
        genre as top_genre,
        genre_watch_seconds,
        row_number() over (partition by user_id order by genre_watch_seconds desc) as rn
    from genre_time
)

select
    d.user_id,
    d.event_date,
    d.total_watch_seconds,
    d.distinct_titles_watched,
    d.completions,
    d.abandons,
    t.top_genre,
    (d.completions = 0 and d.abandons >= 1) as churn_risk_flag
from daily_watch d
left join top_genre t
    on d.user_id = t.user_id and t.rn = 1;
