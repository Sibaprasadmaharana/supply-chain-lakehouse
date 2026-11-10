{{ config(severity='warn') }}
-- Warn when more than 2% of shipments are quarantined: likely an upstream incident.
with totals as (
    select
        count(*) as all_rows,
        count(*) filter (where dq_issue is not null) as bad_rows
    from {{ ref('stg_shipments') }}
)

select *
from totals
where all_rows > 0 and bad_rows * 1.0 / all_rows > 0.02
