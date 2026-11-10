-- Daily executive KPIs on a complete date spine (days with no activity show zeros, not gaps).
with orders as (
    select
        order_date_key as date_key,
        count(distinct order_id) as orders_placed,
        count(distinct case when is_cancelled then order_id end) as orders_cancelled,
        sum(case when not is_cancelled then line_amount else 0 end) as gross_merchandise_value
    from {{ ref('fact_order_items') }}
    group by order_date_key
),

deliveries as (
    select
        delivery_date_key as date_key,
        count(*) as shipments_delivered,
        avg(case when is_on_time then 1.0 else 0.0 end) as on_time_rate,
        avg(delay_days) as avg_delay_days,
        sum(freight_cost) as freight_cost
    from {{ ref('fact_shipments') }}
    where is_delivered
    group by delivery_date_key
),

bounds as (
    select min(order_date_key) as min_key, max(order_date_key) as max_key
    from {{ ref('fact_order_items') }}
)

select
    d.date_key,
    d.date_day,
    d.day_name,
    d.is_weekend,
    coalesce(o.orders_placed, 0) as orders_placed,
    coalesce(o.orders_cancelled, 0) as orders_cancelled,
    coalesce(o.gross_merchandise_value, 0) as gross_merchandise_value,
    coalesce(s.shipments_delivered, 0) as shipments_delivered,
    round(s.on_time_rate, 4) as on_time_rate,
    round(s.avg_delay_days, 2) as avg_delay_days,
    coalesce(s.freight_cost, 0) as freight_cost
from {{ ref('dim_date') }} as d
cross join bounds as b
left join orders as o on d.date_key = o.date_key
left join deliveries as s on d.date_key = s.date_key
where d.date_key between b.min_key and b.max_key
