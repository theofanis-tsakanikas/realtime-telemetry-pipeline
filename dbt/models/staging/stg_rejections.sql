-- Thin staging view over the raw rejections table. event_time is nullable here
-- (a row can be rejected precisely because its timestamp was missing/invalid),
-- so downstream models bucket on coalesce(event_time, ingest_time).
with source as (
    select * from {{ source('telemetry', 'rejections') }}
)

select
    sensor_id,
    rejection_reason,
    event_time,
    ingest_time,
    coalesce(event_time, ingest_time) as bucket_time
from source
