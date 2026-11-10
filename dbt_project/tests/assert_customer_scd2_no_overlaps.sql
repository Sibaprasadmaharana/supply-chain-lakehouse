-- No customer may have two versions whose validity windows overlap.
select a.customer_id, a.customer_sk as sk_a, b.customer_sk as sk_b
from {{ ref('dim_customer') }} as a
inner join {{ ref('dim_customer') }} as b
    on a.customer_id = b.customer_id
    and a.customer_sk < b.customer_sk
    and a.valid_from < b.valid_to
    and b.valid_from < a.valid_to
