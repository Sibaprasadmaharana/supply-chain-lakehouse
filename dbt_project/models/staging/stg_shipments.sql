-- Latest version of each shipment, with a data-quality flag.
-- Rows with dq_issue are routed to quality.quarantine_shipments and excluded downstream.
with source as (
    select * from {{ source('raw', 'shipments') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (partition by shipment_id order by updated_at desc, _loaded_at desc) = 1
)

select
    shipment_id,
    order_id,
    carrier_id,
    shipped_ts,
    delivered_ts,
    cast(freight_cost as decimal(10, 2)) as freight_cost,
    lower(shipment_status) as shipment_status,
    created_at,
    updated_at,
    case
        when freight_cost < 0 then 'negative_freight_cost'
        when delivered_ts < shipped_ts then 'delivered_before_shipped'
        when shipment_status = 'delivered' and delivered_ts is null then 'delivered_without_timestamp'
    end as dq_issue
from deduplicated
