{{ config(contract={'enforced': true}) }}

-- Monthly carrier scorecard: SLA attainment, rolling trend, month-over-month change and ranking.
with deliveries as (
    select
        carrier_id,
        cast(date_trunc('month', delivered_ts) as date) as delivery_month,
        is_on_time,
        transit_days,
        freight_cost,
        shipment_weight_kg
    from {{ ref('fact_shipments') }}
    where is_delivered
),

monthly as (
    select
        carrier_id,
        delivery_month,
        count(*) as shipments_delivered,
        avg(case when is_on_time then 1.0 else 0.0 end) as on_time_rate,
        avg(transit_days) as avg_transit_days,
        sum(freight_cost) as total_freight_cost,
        sum(freight_cost) / nullif(sum(shipment_weight_kg), 0) as freight_cost_per_kg
    from deliveries
    group by carrier_id, delivery_month
)

select
    cast(m.carrier_id as integer) as carrier_id,
    cast(c.carrier_name as varchar) as carrier_name,
    cast(c.service_level as varchar) as service_level,
    cast(m.delivery_month as date) as delivery_month,
    cast(m.shipments_delivered as bigint) as shipments_delivered,
    cast(round(m.on_time_rate, 4) as double) as on_time_rate,
    cast(c.target_on_time_rate as double) as target_on_time_rate,
    cast(m.on_time_rate >= c.target_on_time_rate as boolean) as meets_sla,
    cast(round(avg(m.on_time_rate) over (
        partition by m.carrier_id order by m.delivery_month
        rows between 2 preceding and current row
    ), 4) as double) as on_time_rate_3m_rolling,
    cast(round(m.on_time_rate - lag(m.on_time_rate) over (
        partition by m.carrier_id order by m.delivery_month
    ), 4) as double) as on_time_rate_mom_change,
    cast(rank() over (partition by m.delivery_month order by m.on_time_rate desc) as bigint) as on_time_rank_in_month,
    cast(round(m.avg_transit_days, 2) as double) as avg_transit_days,
    cast(round(m.total_freight_cost, 2) as double) as total_freight_cost,
    cast(round(m.freight_cost_per_kg, 2) as double) as freight_cost_per_kg
from monthly as m
inner join {{ ref('dim_carrier') }} as c
    on m.carrier_id = c.carrier_id
