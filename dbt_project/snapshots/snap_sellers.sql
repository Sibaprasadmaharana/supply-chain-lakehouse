{% snapshot snap_sellers %}
{{
    config(
        target_schema='snapshots',
        unique_key='seller_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}
-- Captures seller changes each time dbt runs (contrast: dim_customer rebuilds history from raw).
select seller_id, seller_name, city, state_code, rating, created_at, updated_at
from {{ ref('stg_sellers') }}
{% endsnapshot %}
