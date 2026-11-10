-- Invalid shipment records held back from the gold layer for investigation.
select
    shipment_id,
    order_id,
    carrier_id,
    shipped_ts,
    delivered_ts,
    freight_cost,
    shipment_status,
    dq_issue,
    updated_at,
    current_timestamp as quarantined_at
from {{ ref('stg_shipments') }}
where dq_issue is not null
