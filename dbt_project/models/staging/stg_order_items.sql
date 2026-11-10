with source as (
    select * from {{ source('raw', 'order_items') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (partition by order_item_id order by updated_at desc, _loaded_at desc) = 1
)

select
    order_item_id,
    order_id,
    product_id,
    seller_id,
    quantity,
    cast(unit_price as decimal(10, 2)) as unit_price,
    created_at,
    updated_at
from deduplicated
