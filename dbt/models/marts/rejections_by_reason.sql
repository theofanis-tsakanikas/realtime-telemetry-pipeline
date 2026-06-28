-- Per-minute breakdown of rejected rows by the contract rule they violated, over
-- the last 24h. Grain = (minute, rejection_reason). Drives the "what is the data
-- quality problem" view — a spike in one reason points straight at the upstream bug.
select
    timestamp_trunc(bucket_time, minute) as minute,
    rejection_reason,
    count(*) as rejection_count
from {{ ref('stg_rejections') }}
where bucket_time >= timestamp_sub(current_timestamp(), interval 24 hour)
group by minute, rejection_reason
