-- Per-minute data-quality KPI: accepted vs rejected counts and the accept rate,
-- over the last 24h. Grain = minute. This is the dbt-modelled twin of the live
-- dq:accept_rate Redis metric — same number, queryable history in BigQuery.
with accepted as (
    select
        timestamp_trunc(event_time, minute) as minute,
        count(*) as accepted_count
    from {{ ref('stg_readings') }}
    where event_time >= timestamp_sub(current_timestamp(), interval 24 hour)
    group by minute
),

rejected as (
    select
        timestamp_trunc(bucket_time, minute) as minute,
        count(*) as rejected_count
    from {{ ref('stg_rejections') }}
    where bucket_time >= timestamp_sub(current_timestamp(), interval 24 hour)
    group by minute
)

select
    minute,
    coalesce(accepted_count, 0) as accepted_count,
    coalesce(rejected_count, 0) as rejected_count,
    coalesce(accepted_count, 0) + coalesce(rejected_count, 0) as total_count,
    safe_divide(
        coalesce(accepted_count, 0),
        coalesce(accepted_count, 0) + coalesce(rejected_count, 0)
    ) as accept_rate
from accepted
full outer join rejected using (minute)
