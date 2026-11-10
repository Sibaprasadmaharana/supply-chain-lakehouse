-- Reconciliation: the incremental fact must hold exactly the clean shipments in staging (catches incremental drift).
with expected as (
    select count(*) as n from {{ ref('stg_shipments') }} where dq_issue is null
),

actual as (
    select count(*) as n from {{ ref('fact_shipments') }}
)

select e.n as expected_rows, a.n as actual_rows
from expected as e
cross join actual as a
where e.n <> a.n
