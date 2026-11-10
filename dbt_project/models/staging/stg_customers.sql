-- Latest version of each customer. Full history is modelled in int_customer_versions.
with source as (
    select * from {{ source('raw', 'customers') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (partition by customer_id order by updated_at desc, _loaded_at desc) = 1
)

select
    customer_id,
    trim(customer_name) as customer_name,
    lower(trim(email)) as email,
    trim(city) as city,
    upper(state) as state_code,
    lower(segment) as customer_segment,
    created_at,
    updated_at
from deduplicated
