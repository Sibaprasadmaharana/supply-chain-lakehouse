{{
    config(
        materialized='incremental',
        unique_key='shipment_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns',
        post_hook="delete from {{ this }} where shipment_id in (select shipment_id from {{ ref('stg_shipments') }} where dq_issue is not null)"
    )
}}

-- Grain: one row per (valid) shipment. Accumulating-snapshot style: a row is
-- re-processed as it moves from in_transit to delivered.
-- The post_hook evicts rows that were clean when loaded but failed a DQ rule on a later update.
with shipments as (
    select * from {{ ref('int_shipments_enriched') }}
    {% if is_incremental() %}
        where record_updated_at >= (
            select max(record_updated_at) - interval {{ var('incremental_lookback_days') }} day
            from {{ this }}
        )
    {% endif %}
)

select
    s.shipment_id,
    s.order_id,
    coalesce(c.customer_sk, '-1') as customer_sk,
    s.customer_id,
    s.carrier_id,
    {{ date_key('s.order_ts') }} as order_date_key,
    {{ date_key('s.shipped_ts') }} as ship_date_key,
    case when s.delivered_ts is not null then {{ date_key('s.delivered_ts') }} end as delivery_date_key,
    s.order_ts,
    s.shipped_ts,
    s.delivered_ts,
    s.promised_delivery_date,
    s.shipment_status,
    s.freight_cost,
    s.shipment_weight_kg,
    s.days_to_ship,
    s.transit_days,
    s.is_delivered,
    s.is_on_time,
    s.delay_days,
    s.record_updated_at
from shipments as s
left join {{ ref('dim_customer') }} as c
    on s.customer_id = c.customer_id
    and s.order_ts >= c.valid_from
    and s.order_ts < c.valid_to
