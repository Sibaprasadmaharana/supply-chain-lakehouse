-- Every customer must have exactly one current version.
select customer_id, count(*) as current_versions
from {{ ref('dim_customer') }}
where is_current
group by customer_id
having count(*) <> 1
