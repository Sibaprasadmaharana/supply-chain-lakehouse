select
    i.order_id,
    sum(i.quantity * p.weight_kg) as order_weight_kg
from {{ ref('stg_order_items') }} as i
inner join {{ ref('stg_products') }} as p
    on i.product_id = p.product_id
group by i.order_id
