with source as (
    select * from {{ source('raw', 'orders') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (partition by order_id order by updated_at desc, _loaded_at desc) = 1
)

select
    order_id,
    customer_id,
    lower(order_status) as order_status,
    order_ts,
    promised_delivery_date,
    created_at,
    updated_at
from deduplicated
