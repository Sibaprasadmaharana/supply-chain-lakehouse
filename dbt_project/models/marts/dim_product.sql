select
    product_id,
    product_name,
    category,
    unit_price as current_unit_price,
    weight_kg
from {{ ref('stg_products') }}
