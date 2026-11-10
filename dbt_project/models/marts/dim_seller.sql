-- SCD Type 2 seller dimension built from a dbt snapshot (compare with the hand-written dim_customer).
with snapshot_rows as (
    select
        *,
        row_number() over (partition by seller_id order by dbt_valid_from) as version_number
    from {{ ref('snap_sellers') }}
)

select
    dbt_scd_id as seller_sk,
    seller_id,
    seller_name,
    city,
    state_code,
    rating as seller_rating,
    version_number,
    case when version_number = 1 then created_at else dbt_valid_from end as valid_from,
    coalesce(dbt_valid_to, timestamp '9999-12-31 00:00:00') as valid_to,
    dbt_valid_to is null as is_current
from snapshot_rows

union all

select
    '-1' as seller_sk,
    -1 as seller_id,
    'Unknown' as seller_name,
    'Unknown' as city,
    'NA' as state_code,
    null as seller_rating,
    1 as version_number,
    timestamp '1900-01-01 00:00:00' as valid_from,
    timestamp '9999-12-31 00:00:00' as valid_to,
    true as is_current
