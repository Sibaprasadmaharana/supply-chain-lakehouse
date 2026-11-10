{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

-- Grain: one row per order line. Point-in-time joins resolve the customer / seller
-- version that was valid when the order was placed.
with items as (
    select
        i.order_item_id,
        i.order_id,
        i.product_id,
        i.seller_id,
        i.quantity,
        i.unit_price,
        o.customer_id,
        o.order_ts,
        o.order_status,
        greatest(i.updated_at, o.updated_at) as record_updated_at
    from {{ ref('stg_order_items') }} as i
    inner join {{ ref('stg_orders') }} as o
        on i.order_id = o.order_id
),

changed as (
    select * from items
    {% if is_incremental() %}
        -- lookback window re-processes recent changes so late-arriving updates are not missed
        where record_updated_at >= (
            select max(record_updated_at) - interval {{ var('incremental_lookback_days') }} day
            from {{ this }}
        )
    {% endif %}
)

select
    f.order_item_id,
    f.order_id,
    coalesce(c.customer_sk, '-1') as customer_sk,
    coalesce(s.seller_sk, '-1') as seller_sk,
    f.product_id,
    f.customer_id,
    f.seller_id,
    {{ date_key('f.order_ts') }} as order_date_key,
    f.order_ts,
    f.order_status,
    f.order_status = 'cancelled' as is_cancelled,
    f.quantity,
    f.unit_price,
    cast(f.quantity * f.unit_price as decimal(12, 2)) as line_amount,
    f.record_updated_at
from changed as f
left join {{ ref('dim_customer') }} as c
    on f.customer_id = c.customer_id
    and f.order_ts >= c.valid_from
    and f.order_ts < c.valid_to
left join {{ ref('dim_seller') }} as s
    on f.seller_id = s.seller_id
    and f.order_ts >= s.valid_from
    and f.order_ts < s.valid_to
