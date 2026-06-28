-- accept_rate is a ratio and must always fall in [0, 1]. Any row outside that
-- band is a logic error in the mart (e.g. double-counting). Returns offending rows.
select
    minute,
    accept_rate
from {{ ref('accept_rate_minutely') }}
where accept_rate < 0 or accept_rate > 1
