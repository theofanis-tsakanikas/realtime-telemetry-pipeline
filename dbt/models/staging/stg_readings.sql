-- Thin staging view over the raw readings table: explicit column list so the
-- contract is visible here, and downstream marts never depend on source layout.
with source as (
    select * from {{ source('telemetry', 'readings') }}
)

select
    sensor_id,
    temperature,
    humidity,
    pressure,
    event_time,
    ingest_time
from source
