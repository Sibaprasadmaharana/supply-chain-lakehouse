-- Hand-written SCD Type 2 built from the append-only raw change log.
-- Because raw keeps every captured version, this model is fully rebuildable (unlike a snapshot).
-- Tracked (Type 2) attributes: city, state, segment. Name / e-mail are Type 1 (always current).
with raw_versions as (
    -- the ingestion lookback re-reads rows, so identical versions can appear more than once
    select distinct
        customer_id,
        city,
        upper(state) as state_code,
        lower(segment) as customer_segment,
        created_at,
        updated_at
    from {{ source('raw', 'customers') }}
),

hashed as (
    select
        *,
        md5(concat_ws('|', city, state_code, customer_segment)) as attribute_hash
    from raw_versions
),

with_previous as (
    select
        *,
        lag(attribute_hash) over (partition by customer_id order by updated_at) as previous_hash
    from hashed
),

-- keep only real changes: drops no-op updates and Type 1-only changes (e.g. e-mail)
changes as (
    select *
    from with_previous
    where previous_hash is null or attribute_hash <> previous_hash
),

windowed as (
    select
        *,
        row_number() over (partition by customer_id order by updated_at) as version_number,
        lead(updated_at) over (partition by customer_id order by updated_at) as next_valid_from
    from changes
)

select
    md5(cast(customer_id as varchar) || '|' || cast(updated_at as varchar)) as customer_sk,
    customer_id,
    city,
    state_code,
    customer_segment,
    version_number,
    case when version_number = 1 then created_at else updated_at end as valid_from,
    coalesce(next_valid_from, timestamp '9999-12-31 00:00:00') as valid_to,
    next_valid_from is null as is_current
from windowed
