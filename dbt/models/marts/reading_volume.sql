-- Fleet-wide ingestion throughput over the last 6h: readings per minute and how
-- many distinct sensors were active. Grain = minute. Surfaces a stalled producer
-- (volume drop) or a sensor dropping offline (active_sensors < 5).
select
    timestamp_trunc(event_time, minute) as minute,
    count(*) as reading_count,
    count(distinct sensor_id) as active_sensors
from {{ ref('stg_readings') }}
where event_time >= timestamp_sub(current_timestamp(), interval 6 hour)
group by minute
