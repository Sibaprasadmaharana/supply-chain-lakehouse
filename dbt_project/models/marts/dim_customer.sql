-- SCD Type 2 customer dimension (history from int_customer_versions, Type 1 attributes from stg_customers).
with versions as (
    select * from {{ ref('int_customer_versions') }}
),

current_attributes as (
    select * from {{ ref('stg_customers') }}
)

select
    v.customer_sk,
    v.customer_id,
    c.customer_name,
    c.email,
    v.city,
    v.state_code,
    v.customer_segment,
    v.version_number,
    v.valid_from,
    v.valid_to,
    v.is_current
from versions as v
inner join current_attributes as c
    on v.customer_id = c.customer_id

union all

-- unknown member: facts whose customer cannot be resolved point-in-time land here instead of being dropped
select
    '-1' as customer_sk,
    -1 as customer_id,
    'Unknown' as customer_name,
    null as email,
    'Unknown' as city,
    'NA' as state_code,
    'unknown' as customer_segment,
    1 as version_number,
    timestamp '1900-01-01 00:00:00' as valid_from,
    timestamp '9999-12-31 00:00:00' as valid_to,
    true as is_current
