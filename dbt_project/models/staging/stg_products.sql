with source as (
    select * from {{ source('raw', 'products') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (partition by product_id order by updated_at desc, _loaded_at desc) = 1
)

select
    product_id,
    trim(product_name) as product_name,
    lower(category) as category,
    cast(unit_price as decimal(10, 2)) as unit_price,
    cast(weight_kg as decimal(7, 2)) as weight_kg,
    created_at,
    updated_at
from deduplicated
