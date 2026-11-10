select
    c.carrier_id,
    c.carrier_name,
    c.service_level,
    t.target_on_time_rate
from {{ ref('stg_carriers') }} as c
left join {{ ref('carrier_sla_targets') }} as t
    on c.service_level = t.service_level
