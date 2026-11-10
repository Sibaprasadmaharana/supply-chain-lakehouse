with source as (
    select * from {{ source('raw', 'carriers') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (partition by carrier_id order by updated_at desc, _loaded_at desc) = 1
)

select
    carrier_id,
    trim(carrier_name) as carrier_name,
    lower(service_level) as service_level,
    created_at,
    updated_at
from deduplicated
