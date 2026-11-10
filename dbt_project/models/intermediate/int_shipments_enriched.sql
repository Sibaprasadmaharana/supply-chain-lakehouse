-- Business logic for delivery performance lives here (and is unit-tested).
with shipments as (
    select * from {{ ref('stg_shipments') }}
    where dq_issue is null
),

orders as (
    select * from {{ ref('stg_orders') }}
),

weights as (
    select * from {{ ref('int_order_weights') }}
)

select
    s.shipment_id,
    s.order_id,
    o.customer_id,
    s.carrier_id,
    o.order_ts,
    o.promised_delivery_date,
    s.shipped_ts,
    s.delivered_ts,
    s.shipment_status,
    s.freight_cost,
    w.order_weight_kg as shipment_weight_kg,
    date_diff('day', cast(o.order_ts as date), cast(s.shipped_ts as date)) as days_to_ship,
    case
        when s.delivered_ts is not null
            then round(date_diff('minute', s.shipped_ts, s.delivered_ts) / 1440.0, 2)
    end as transit_days,
    s.delivered_ts is not null as is_delivered,
    case
        when s.delivered_ts is not null
            then cast(s.delivered_ts as date) <= o.promised_delivery_date
    end as is_on_time,
    case
        when s.delivered_ts is not null
            then greatest(date_diff('day', o.promised_delivery_date, cast(s.delivered_ts as date)), 0)
    end as delay_days,
    greatest(s.updated_at, o.updated_at) as record_updated_at
from shipments as s
left join orders as o
    on s.order_id = o.order_id
left join weights as w
    on s.order_id = w.order_id
