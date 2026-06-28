-- Grain guard: at most one row per (sensor_id, minute). Returns any duplicated
-- grain keys (zero rows = pass).
select
    sensor_id,
    minute,
    count(*) as n
from {{ ref('sensor_minutely') }}
group by sensor_id, minute
having count(*) > 1
