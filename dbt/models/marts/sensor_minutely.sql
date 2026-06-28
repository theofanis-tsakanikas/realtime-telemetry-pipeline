-- Per-sensor, per-minute rollup of the last 24h of readings: the core BI table
-- behind the "all sensors" trend panels. Grain = (sensor_id, minute).
select
    sensor_id,
    timestamp_trunc(event_time, minute) as minute,
    count(*) as reading_count,
    avg(temperature) as avg_temperature,
    min(temperature) as min_temperature,
    max(temperature) as max_temperature,
    avg(humidity) as avg_humidity,
    avg(pressure) as avg_pressure
from {{ ref('stg_readings') }}
where event_time >= timestamp_sub(current_timestamp(), interval 24 hour)
group by sensor_id, minute
